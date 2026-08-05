"""
Unit tests for the bridge batching + offset commit logic (no broker needed).
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from bridge import PendingBatch

from shared.connectors.kafka import KafkaConsumerClient, KafkaProducerClient


class FakeMessage:
    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: list = []

    def commit(self, message=None, asynchronous: bool = False) -> None:
        self.committed.append(message)


class FakeProducer:
    def __init__(self, remaining: int = 0) -> None:
        self.remaining = remaining

    def flush(self, timeout: float) -> int:
        return self.remaining


def _consumer_client(fake) -> KafkaConsumerClient:
    client = KafkaConsumerClient.__new__(KafkaConsumerClient)
    client._consumer = fake
    return client


def _producer_client(fake) -> KafkaProducerClient:
    client = KafkaProducerClient.__new__(KafkaProducerClient)
    client._producer = fake
    return client


# ── PendingBatch ──────────────────────────────────────────────────────


def test_empty_batch_never_flushes():
    # a timeout trigger on an empty buffer would commit nothing and reset the clock
    batch = PendingBatch(batch_size=10, flush_interval=0.0)
    assert batch.should_flush() is False


def test_flushes_when_batch_is_full():
    batch = PendingBatch(batch_size=3, flush_interval=999.0)
    for i in range(2):
        batch.append(FakeMessage(0, i))
    assert batch.should_flush() is False

    batch.append(FakeMessage(0, 2))
    assert batch.should_flush() is True


def test_flushes_when_interval_elapsed():
    batch = PendingBatch(batch_size=999, flush_interval=0.05)
    batch.append(FakeMessage(0, 0))
    assert batch.should_flush() is False

    time.sleep(0.06)
    assert batch.should_flush() is True


def test_interval_measured_from_oldest_pending_message():
    # a long idle gap before the first message must not force an immediate flush
    batch = PendingBatch(batch_size=999, flush_interval=0.05)
    time.sleep(0.06)
    batch.append(FakeMessage(0, 0))
    assert batch.should_flush() is False


def test_drain_returns_all_and_empties():
    batch = PendingBatch(batch_size=999, flush_interval=999.0)
    msgs = [FakeMessage(0, i) for i in range(3)]
    for msg in msgs:
        batch.append(msg)

    assert batch.drain() == msgs
    assert len(batch) == 0
    assert batch.should_flush() is False


# ── commit_batch ──────────────────────────────────────────────────────


def test_commit_batch_commits_last_offset_of_every_partition():
    # commit(message=...) only advances that message's own partition
    fake = FakeConsumer()
    msgs = [
        FakeMessage(0, 10), FakeMessage(1, 5), FakeMessage(0, 11),
        FakeMessage(2, 7), FakeMessage(1, 6),
    ]

    _consumer_client(fake).commit_batch(msgs)

    committed = {msg.partition(): msg.offset() for msg in fake.committed}
    assert committed == {0: 11, 1: 6, 2: 7}


def test_commit_batch_is_noop_on_empty_list():
    fake = FakeConsumer()
    _consumer_client(fake).commit_batch([])
    assert fake.committed == []


# ── flush ─────────────────────────────────────────────────────────────


def test_flush_passes_when_queue_is_drained():
    _producer_client(FakeProducer(remaining=0)).flush(timeout=0.1)


def test_flush_raises_when_messages_are_not_acked():
    # silently ignoring this would let the caller commit offsets for lost messages
    with pytest.raises(RuntimeError, match="not acked"):
        _producer_client(FakeProducer(remaining=2)).flush(timeout=0.1)
