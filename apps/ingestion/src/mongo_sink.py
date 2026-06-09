"""
MongoDB Sink: consume from user-events, batch-insert into raw_events collection.

Commit Kafka offset only after MongoDB insert succeeds (at-least-once delivery).
Sequence: buffer event → batch full or timeout → insert_many(ordered=False) → commit offset

Usage:
    poetry run python apps/ingestion/src/mongo_sink.py
"""
import json
import signal
import time
from datetime import datetime, timezone

from pymongo.errors import BulkWriteError

from shared.config.settings import settings
from shared.connectors.kafka import make_sink_consumer
from shared.connectors.mongodb import make_mongo_client
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

    def _commit_all_partitions(self, consumer, msgs: list) -> None:
        last_per_partition: dict[int, object] = {}
        for msg in msgs:
            last_per_partition[msg.partition()] = msg
        for msg in last_per_partition.values():
            consumer.commit(message=msg)

    def write(self, docs: list[dict], consumer, msgs: list) -> None:
        for attempt in range(MAX_RETRIES):
            try:
                # ordered=False: one bad doc doesn't abort the rest of the batch
                self._collection.insert_many(docs, ordered=False)
                self._commit_all_partitions(consumer, msgs)
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
                self._commit_all_partitions(consumer, msgs)
                return
            except Exception as e:
                log.warning(f"write failed attempt={attempt + 1}: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # 1s, 2s, 4s


class MongoSink:
    def __init__(self) -> None:
        self._running = True
        self._consumer = make_sink_consumer(settings, CONSUMER_GROUP)
        mongo_client = make_mongo_client(settings)
        collection = mongo_client.get_collection(COLLECTION)
        self._buffer = EventBuffer(settings.BATCH_SIZE, settings.FLUSH_INTERVAL_SECONDS)
        self._writer = MongoDBWriter(collection)

    def run(self) -> None:
        self._consumer.subscribe(topics=[settings.SINK_KAFKA_TOPIC])
        log.info(f"mongo_sink started — consuming from {settings.SINK_KAFKA_TOPIC}")

        idle_polls = 0
        while self._running:
            msg = self._consumer.poll()
            if msg is None:
                idle_polls += 1
                if idle_polls % 30 == 0:
                    log.info(f"still running — idle for ~{idle_polls}s")
                # flush on idle too — time-based trigger fires most often when traffic is low
                if self._buffer.should_flush():
                    docs, msgs = self._buffer.flush()
                    if docs:  # guard: timeout can fire on empty buffer
                        self._writer.write(docs, self._consumer, msgs)
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
                docs, msgs = self._buffer.flush()
                if docs:
                    self._writer.write(docs, self._consumer, msgs)


if __name__ == "__main__":
    sink = MongoSink()
    signal.signal(signal.SIGINT, lambda *_: setattr(sink, "_running", False))
    signal.signal(signal.SIGTERM, lambda *_: setattr(sink, "_running", False))
    sink.run()
