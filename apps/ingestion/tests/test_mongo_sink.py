"""
Unit tests for the mongo sink shutdown path (no broker, no MongoDB needed).

A graceful stop must write the partial batch and commit its offsets before the
consumer leaves the group; any stop must release the consumer and the MongoDB client.
"""
import json
import sys
from pathlib import Path

import pytest
from pymongo.errors import BulkWriteError

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
import mongo_sink
from mongo_sink import EventBuffer, MongoDBWriter, MongoSink

from shared.connectors.kafka import KafkaConsumerClient


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return json.dumps({"id": f"evt-{self._partition}-{self._offset}"}).encode()

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    """Stands in for confluent_kafka.Consumer: hands out queued messages, then None."""

    def __init__(self, msgs: list, on_drained=None) -> None:
        self._queue = list(msgs)
        self._on_drained = on_drained
        self.committed: list = []
        self.closed = False

    def subscribe(self, topics, on_assign=None) -> None:
        on_assign(self, [])

    def poll(self, timeout: float):
        if self._queue:
            return self._queue.pop(0)
        if self._on_drained:
            self._on_drained()
        return None

    def commit(self, message=None, asynchronous: bool = False) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


class FakeCollection:
    def __init__(self, error: Exception | None = None) -> None:
        self.inserted: list[dict] = []
        self._error = error

    def insert_many(self, docs: list[dict], ordered: bool = True) -> None:
        if self._error is not None:
            raise self._error
        self.inserted.extend(docs)


class FakeMongoClient:
    def __init__(self, collection: FakeCollection) -> None:
        self._collection = collection
        self.closed = False

    def get_collection(self, name: str) -> FakeCollection:
        return self._collection

    def close(self) -> None:
        self.closed = True


def _consumer_client(fake: FakeConsumer) -> KafkaConsumerClient:
    client = KafkaConsumerClient.__new__(KafkaConsumerClient)
    client._consumer = fake
    client._assigned = False
    return client


def _make_sink(msgs: list, collection: FakeCollection, batch_size: int = 999):
    """Sink whose consumer raises the stop flag once its queue is empty, like a SIGINT."""
    fake_consumer = FakeConsumer(msgs)
    mongo_client = FakeMongoClient(collection)
    sink = MongoSink(
        consumer=_consumer_client(fake_consumer),
        mongo_client=mongo_client,
        buffer=EventBuffer(batch_size=batch_size, flush_interval=999.0),
    )
    fake_consumer._on_drained = lambda: setattr(sink, "_running", False)
    return sink, fake_consumer, mongo_client


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(mongo_sink.time, "sleep", lambda _: None)


# ── graceful stop ─────────────────────────────────────────────────────


def test_stop_mid_batch_writes_and_commits_the_partial_batch():
    msgs = [FakeMessage(0, 10), FakeMessage(1, 4), FakeMessage(0, 11)]
    collection = FakeCollection()
    sink, consumer, _ = _make_sink(msgs, collection)

    sink.run()

    assert [doc["id"] for doc in collection.inserted] == ["evt-0-10", "evt-1-4", "evt-0-11"]
    committed = {msg.partition(): msg.offset() for msg in consumer.committed}
    assert committed == {0: 11, 1: 4}


def test_partial_batch_is_written_before_the_consumer_closes():
    # closing first would leave the group and lose the right to commit those offsets
    order: list[str] = []
    collection = FakeCollection()
    collection.insert_many = lambda docs, ordered=True: order.append("insert")
    sink, consumer, _ = _make_sink([FakeMessage(0, 0)], collection)
    consumer.close = lambda: order.append("close")

    sink.run()

    assert order == ["insert", "close"]


def test_stop_releases_consumer_and_mongo_client():
    sink, consumer, mongo_client = _make_sink([FakeMessage(0, 0)], FakeCollection())

    sink.run()

    assert consumer.closed is True
    assert mongo_client.closed is True


def test_stop_with_empty_buffer_writes_nothing():
    collection = FakeCollection()
    sink, consumer, mongo_client = _make_sink([], collection)

    sink.run()

    assert collection.inserted == []
    assert consumer.committed == []
    assert consumer.closed is True
    assert mongo_client.closed is True


def test_stop_after_a_full_batch_does_not_rewrite_it():
    msgs = [FakeMessage(0, i) for i in range(3)]
    collection = FakeCollection()
    sink, consumer, _ = _make_sink(msgs, collection, batch_size=2)

    sink.run()

    # one full batch of 2 in the loop, the remaining 1 on shutdown, nothing twice
    assert [doc["_kafka_offset"] for doc in collection.inserted] == [0, 1, 2]
    assert [msg.offset() for msg in consumer.committed] == [1, 2]


def test_duplicate_only_final_write_still_commits():
    # a replayed partial batch is already in MongoDB: E11000 only means "done before"
    dup = BulkWriteError({"writeErrors": [{"code": 11000}]})
    sink, consumer, _ = _make_sink([FakeMessage(0, 7)], FakeCollection(error=dup))

    sink.run()

    assert [msg.offset() for msg in consumer.committed] == [7]
    assert consumer.closed is True


# ── failed stop ───────────────────────────────────────────────────────


def test_failed_final_write_does_not_commit_but_still_releases_everything():
    sink, consumer, mongo_client = _make_sink(
        [FakeMessage(0, 0)], FakeCollection(error=ConnectionError("mongo down"))
    )

    with pytest.raises(ConnectionError, match="mongo down"):
        sink.run()

    assert consumer.committed == []
    assert consumer.closed is True
    assert mongo_client.closed is True


def test_mongo_client_closes_even_when_consumer_close_raises():
    sink, consumer, mongo_client = _make_sink([], FakeCollection())

    def broken_close():
        raise RuntimeError("close failed")

    consumer.close = broken_close

    with pytest.raises(RuntimeError, match="close failed"):
        sink.run()

    assert mongo_client.closed is True


def test_crash_in_the_loop_releases_without_writing_the_rest():
    # a failed in-loop write must surface; a retry from the shutdown path would mask it
    class FailingWriter(MongoDBWriter):
        def __init__(self) -> None:
            self.calls = 0

        def write(self, docs, consumer, msgs) -> None:
            self.calls += 1
            raise ConnectionError("mongo down")

    fake_consumer = FakeConsumer([FakeMessage(0, 0), FakeMessage(0, 1)])
    mongo_client = FakeMongoClient(FakeCollection())
    writer = FailingWriter()
    sink = MongoSink(
        consumer=_consumer_client(fake_consumer),
        mongo_client=mongo_client,
        buffer=EventBuffer(batch_size=1, flush_interval=999.0),
        writer=writer,
    )

    with pytest.raises(ConnectionError, match="mongo down"):
        sink.run()

    assert writer.calls == 1
    assert fake_consumer.committed == []
    assert fake_consumer.closed is True
    assert mongo_client.closed is True
