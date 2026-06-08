"""
Bridge: consume from source Kafka, validate, route to sink topic or DLQ.

Commit source offset only after produce succeeds (at-least-once delivery).
Sequence: consume → validate → produce → flush → commit

Usage:
    poetry run python apps/ingestion/src/bridge.py
"""
import json
import signal
from datetime import datetime, timezone

from shared.config.settings import settings
from shared.connectors.kafka import make_sink_producer, make_source_consumer
from shared.utils.logger import get_logger
from validator import SchemaValidator

log = get_logger("bridge")


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

    def run(self) -> None:
        # Subscribe source consumer to SOURCE_KAFKA_TOPIC
        self._source_consumer.subscribe(topics=[settings.SOURCE_KAFKA_TOPIC])
        log.info(f"bridge started — consuming from {settings.SOURCE_KAFKA_TOPIC}")

        # Poll loop
        idle_polls = 0
        while self._running:
            # Pull message
            msg = self._source_consumer.poll()
            if msg is None:
                # Heartbeat — confirms the loop is alive while waiting for events
                idle_polls += 1
                if idle_polls % 30 == 0:
                    log.info(f"still running — idle for ~{idle_polls}s")
                continue
            if msg.error():
                log.error(msg.error())
                continue

            idle_polls = 0

            # Validate & route
            result = self._validator.validate(raw_bytes=msg.value())
            self._route(msg, result)

            # Commit offset
            self._source_consumer.commit(message=msg)
            log.info(f"processed offset={msg.offset()} valid={result.is_valid}")

    def _route(self, msg, result) -> None:
        """Send valid messages to sink topic, invalid ones to DLQ."""
        if result.is_valid:
            # Forward original bytes as-is
            self._sink_producer.produce(
                topic=settings.SINK_KAFKA_TOPIC,
                value=msg.value(),
            )
            self._sink_producer.flush()
        else:
            # Reuse parsed payload when available; fall back to raw bytes
            # (json_parse_error never produces a payload)
            if result.payload is not None:
                dlq_payload = result.payload
            else:
                dlq_payload = {"_raw": msg.value().decode("utf-8", errors="replace")}

            dlq_value = _add_dlq_metadata(dlq_payload, result)
            self._dlq_producer.produce(
                topic=settings.SINK_KAFKA_DLQ_TOPIC,
                value=dlq_value,
            )
            self._dlq_producer.flush()
            log.info(
                f"routed_to_dlq reason={result.error_reason}"
                f" field={result.error_field} offset={msg.offset()}"
            )


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
