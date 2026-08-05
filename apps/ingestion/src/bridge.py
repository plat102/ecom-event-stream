"""
Bridge: consume from source Kafka, validate, route to sink topic or DLQ.

Produce is asynchronous; source offsets are committed only after the whole batch is
flushed and every delivery is acked (at-least-once delivery).
Sequence: consume → validate → produce → [batch full or timeout] → flush → commit

Usage:
    poetry run python apps/ingestion/src/bridge.py
"""
import json
import signal
import time
from datetime import datetime, timezone

from shared.config.settings import settings
from shared.connectors.kafka import make_sink_producer, make_source_consumer
from shared.utils.logger import get_logger
from validator import SchemaValidator

log = get_logger("bridge")


class PendingBatch:
    """Source messages already produced but not yet acked — their offsets can't advance."""

    def __init__(self, batch_size: int, flush_interval: float) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._msgs: list = []
        self._oldest_at = 0.0

    def append(self, msg) -> None:
        if not self._msgs:
            # interval is measured from the oldest pending message, not the last flush
            self._oldest_at = time.time()
        self._msgs.append(msg)

    def should_flush(self) -> bool:
        # two triggers: batch full (throughput) or timeout (bounds the replay window)
        if not self._msgs:
            return False
        return (
            len(self._msgs) >= self.batch_size
            or (time.time() - self._oldest_at) >= self.flush_interval
        )

    def drain(self) -> list:
        msgs, self._msgs = self._msgs, []
        return msgs

    def __len__(self) -> int:
        return len(self._msgs)


class Bridge:
    def __init__(self) -> None:
        self._running = True

        # Source consumer
        self._source_consumer = make_source_consumer(settings)

        # Sink producers
        self._sink_producer = make_sink_producer(settings)
        self._dlq_producer = make_sink_producer(settings)

        # Validator
        self._validator = SchemaValidator()

        # Offsets held back until the produced batch is acked
        self._pending = PendingBatch(
            settings.BRIDGE_BATCH_SIZE, settings.BRIDGE_FLUSH_INTERVAL_SECONDS
        )
        self._delivery_errors: list[str] = []

    def run(self) -> None:
        # Subscribe source consumer to SOURCE_KAFKA_TOPIC
        self._source_consumer.subscribe(topics=[settings.SOURCE_KAFKA_TOPIC])
        self._source_consumer.wait_for_assignment()
        log.info(f"bridge started - consuming from {settings.SOURCE_KAFKA_TOPIC}")

        # Poll loop
        idle_polls = 0
        while self._running:
            # Pull message
            msg = self._source_consumer.poll()
            if msg is None:
                # Confirms the loop is alive while waiting for events
                idle_polls += 1
                if idle_polls % 30 == 0:
                    log.info(f"still running - idle for ~{idle_polls}s")
                # flush on idle too - the timeout trigger fires most often when traffic is low
                if self._pending.should_flush():
                    self._flush_and_commit()
                continue
            if msg.error():
                log.error(msg.error())
                continue

            idle_polls = 0

            # Validate & route
            result = self._validator.validate(raw_bytes=msg.value())
            self._route(msg, result)
            self._pending.append(msg)

            if self._pending.should_flush():
                self._flush_and_commit()

        self._shutdown()

    def _route(self, msg, result) -> None:
        """Send valid messages to sink topic, invalid ones to DLQ."""
        if result.is_valid:
            # Forward original bytes
            self._sink_producer.produce(
                topic=settings.SINK_KAFKA_TOPIC,
                value=msg.value(),
                on_delivery=self._on_delivery,
            )
            self._sink_producer.poll()
        else:
            # Reuse parsed payload when available; fall back to raw bytes
            if result.payload is not None:
                dlq_payload = result.payload
            else:
                dlq_payload = {"_raw": msg.value().decode("utf-8", errors="replace")}

            dlq_value = _add_dlq_metadata(dlq_payload, result)
            self._dlq_producer.produce(
                topic=settings.SINK_KAFKA_DLQ_TOPIC,
                value=dlq_value,
                on_delivery=self._on_delivery,
            )
            self._dlq_producer.poll()
            log.info(
                f"routed_to_dlq reason={result.error_reason}"
                f" field={result.error_field} offset={msg.offset()}"
            )

    def _on_delivery(self, err, msg) -> None:
        """Called while polling/flushing — only records, so the poll loop raises instead."""
        if err is not None:
            self._delivery_errors.append(str(err))
            log.error(f"produce_failed topic={msg.topic()} error={err}")

    def _flush_and_commit(self) -> None:
        """Advance source offsets only once the batch is durably in the sink cluster.

        A failed delivery raises before anything is committed, so the batch is replayed
        on restart instead of being skipped.
        """
        self._sink_producer.flush()
        self._dlq_producer.flush()
        if self._delivery_errors:
            raise RuntimeError(
                f"{len(self._delivery_errors)} produce failure(s),"
                f" first: {self._delivery_errors[0]}"
            )

        msgs = self._pending.drain()
        self._source_consumer.commit_batch(msgs)
        log.info(f"committed {len(msgs)} message(s)")

    def _shutdown(self) -> None:
        """Drain what's pending so a graceful stop doesn't replay the last batch."""
        try:
            if len(self._pending):
                self._flush_and_commit()
        finally:
            self._source_consumer.close()
            log.info("bridge stopped")


def _add_dlq_metadata(payload: dict, result) -> bytes:
    payload["_dlq_reason"] = result.error_reason
    payload["_dlq_field"] = result.error_field
    payload["_dlq_timestamp"] = datetime.now(timezone.utc).isoformat()
    return json.dumps(payload).encode()


if __name__ == "__main__":
    bridge = Bridge()
    signal.signal(signal.SIGINT, lambda *_: setattr(bridge, "_running", False))
    signal.signal(signal.SIGTERM, lambda *_: setattr(bridge, "_running", False))
    bridge.run()
