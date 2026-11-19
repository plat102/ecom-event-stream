"""
MongoDB Sink: consume from user-events, batch-insert into raw_events collection.

Kafka offsets are committed only after the batch is in MongoDB (at-least-once delivery):
insert_many succeeded, or failed on duplicate keys only (a replayed batch). The commit
covers the last offset of every partition in the batch. A batch that still fails after
retries raises before the commit, so it is re-read on restart.
Sequence: buffer event → [batch full or timeout] → insert_many(ordered=False) → commit

SIGINT/SIGTERM stops the loop; the partial batch is written and committed, then the
consumer leaves the group and the MongoDB client closes. After a crash nothing more is
written; the uncommitted events are re-read on restart.

Usage:
    poetry run python apps/ingestion/src/mongo_sink.py
"""
import json
import signal
import time
from datetime import datetime, timezone

from pymongo.errors import BulkWriteError

from shared.config.settings import settings
from shared.connectors.kafka import KafkaConsumerClient, make_sink_consumer
from shared.connectors.mongodb import MongoDBClient, make_mongo_client
from shared.utils.logger import get_logger

log = get_logger("mongo_sink")

CONSUMER_GROUP = "mongo-sink"
COLLECTION = "raw_events"
MAX_RETRIES = 3


class EventBuffer:
    def __init__(self, batch_size: int, flush_interval: float) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._docs: list[dict] = []
        self._msgs: list = []
        self._last_flush = time.time()

    def append(self, doc: dict, msg) -> None:
        # _docs[i] and _msgs[i] always correspond to the same event
        self._docs.append(doc)
        self._msgs.append(msg)

    def should_flush(self) -> bool:
        # two triggers: batch full (throughput) or timeout (latency ceiling)
        return (
            len(self._docs) >= self.batch_size
            or (time.time() - self._last_flush) >= self.flush_interval
        )

    def flush(self) -> tuple[list[dict], list]:
        # swap out the current lists so the next batch starts fresh immediately
        docs, msgs = self._docs, self._msgs
        self._docs, self._msgs = [], []
        self._last_flush = time.time()
        return docs, msgs


class MongoDBWriter:
    def __init__(self, collection) -> None:
        self._collection = collection

    def write(self, docs: list[dict], consumer, msgs: list) -> None:
        for attempt in range(MAX_RETRIES):
            try:
                # ordered=False: one bad doc doesn't abort the rest of the batch
                self._collection.insert_many(docs, ordered=False)
                consumer.commit_batch(msgs)
                log.info(f"inserted {len(docs)} docs")
                return

            except BulkWriteError as e:
                # With ordered=False, non-duplicate docs were already inserted.
                # Duplicate key means the doc is already in MongoDB - treat as success.
                non_dup = [err for err in e.details.get("writeErrors", []) if err.get("code") != 11000]
                if non_dup:
                    raise
                dup_count = len(e.details.get("writeErrors", []))
                log.warning(f"skipped {dup_count} duplicate doc(s), committing offset")
                consumer.commit_batch(msgs)
                return

            except Exception as e:
                log.warning(f"write failed attempt={attempt + 1}: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # 1s, 2s, 4s


class MongoSink:
    def __init__(
        self,
        consumer: KafkaConsumerClient | None = None,
        mongo_client: MongoDBClient | None = None,
        buffer: EventBuffer | None = None,
        writer: MongoDBWriter | None = None,
    ) -> None:
        # defaults build the real clients; tests inject fakes
        self._running = True
        if consumer is None:
            consumer = make_sink_consumer(settings, CONSUMER_GROUP)
        if mongo_client is None:
            mongo_client = make_mongo_client(settings)
        if buffer is None:
            buffer = EventBuffer(settings.BATCH_SIZE, settings.FLUSH_INTERVAL_SECONDS)
        if writer is None:
            writer = MongoDBWriter(mongo_client.get_collection(COLLECTION))
        self._consumer = consumer
        self._mongo_client = mongo_client  # kept so shutdown can close it
        self._buffer = buffer
        self._writer = writer

    def run(self) -> None:
        try:
            self._consumer.subscribe(topics=[settings.SINK_KAFKA_TOPIC])
            self._consumer.wait_for_assignment()
            log.info(f"mongo_sink started — consuming from {settings.SINK_KAFKA_TOPIC}")
            self._poll_loop()
            # only a clean stop drains: after a crash, retrying the write would mask the error
            self._drain()
        finally:
            self._close()

    def _poll_loop(self) -> None:
        idle_polls = 0
        while self._running:
            msg = self._consumer.poll()
            if msg is None:
                idle_polls += 1
                if idle_polls % 30 == 0:
                    log.info(f"still running — idle for ~{idle_polls}s")
                # flush on idle too — time-based trigger fires most often when traffic is low
                if self._buffer.should_flush():
                    self._write_buffer()
                continue
            if msg.error():
                log.error(msg.error())
                continue

            idle_polls = 0

            doc = json.loads(msg.value())
            # inject sink-side metadata not present in the original event
            doc["_kafka_partition"] = msg.partition()
            doc["_kafka_offset"] = msg.offset()
            doc["_ingested_at"] = datetime.now(timezone.utc)

            self._buffer.append(doc, msg)

            if self._buffer.should_flush():
                self._write_buffer()

    def _write_buffer(self) -> int:
        docs, msgs = self._buffer.flush()
        if docs:  # guard: timeout can fire on empty buffer
            self._writer.write(docs, self._consumer, msgs)
        return len(docs)

    def _drain(self) -> None:
        """Write the partial batch while the consumer still owns its partitions.

        Without this, a stop discards up to a batch of buffered events; their offsets are
        uncommitted, so they are re-read on restart (no loss), but that work is wasted.
        """
        written = self._write_buffer()
        if written:
            log.info(f"drained {written} buffered doc(s) on shutdown")

    def _close(self) -> None:
        """Leave the consumer group now rather than after the session timeout."""
        try:
            self._consumer.close()
        finally:
            self._mongo_client.close()
            log.info("mongo_sink stopped")


if __name__ == "__main__":
    sink = MongoSink()
    signal.signal(signal.SIGINT, lambda *_: setattr(sink, "_running", False))
    signal.signal(signal.SIGTERM, lambda *_: setattr(sink, "_running", False))
    sink.run()
