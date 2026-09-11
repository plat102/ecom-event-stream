"""Unit tests for the Kafka admin client: lag arithmetic, group state, topic filter."""
import pytest
from confluent_kafka import TopicPartition

from shared.connectors.kafka_admin import OFFSET_INVALID, KafkaAdminClient

TOPIC = "user-events"
GROUP = "mongo-sink"


class FakeError:
    def __str__(self) -> str:
        return "unknown topic"


class FakeTopicMetadata:
    def __init__(self, partitions: list[int], error=None) -> None:
        self.partitions = {p: object() for p in partitions}
        self.error = error


class FakeClusterMetadata:
    def __init__(self, topics: dict) -> None:
        self.topics = topics


class FakeFuture:
    def __init__(self, value) -> None:
        self._value = value

    def result(self, timeout=None):
        return self._value


class FakeOffsets:
    def __init__(self, topic_partitions: list[TopicPartition]) -> None:
        self.topic_partitions = topic_partitions


class FakeState:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeGroupDescription:
    def __init__(self, state: str, members: int) -> None:
        self.state = FakeState(state)
        self.members = [object()] * members


class FakeAdmin:
    def __init__(self, topics: dict, committed: dict | None = None, group=None) -> None:
        self._topics = topics
        self._committed = committed or {}
        self._group = group

    def list_topics(self, topic=None, timeout=None):
        if topic is None:
            return FakeClusterMetadata(self._topics)
        found = {topic: self._topics[topic]} if topic in self._topics else {}
        return FakeClusterMetadata(found)

    def list_consumer_group_offsets(self, requests):
        request = requests[0]
        partitions = [
            TopicPartition(TOPIC, p, self._committed.get(p, OFFSET_INVALID))
            for p in sorted(self._committed) or []
        ]
        return {request.group_id: FakeFuture(FakeOffsets(partitions))}

    def describe_consumer_groups(self, groups):
        return {groups[0]: FakeFuture(self._group)}


class FakeConsumer:
    def __init__(self, watermarks: dict) -> None:
        self._watermarks = watermarks

    def get_watermark_offsets(self, topic_partition, timeout=None, cached=False):
        return 0, self._watermarks[topic_partition.partition]


def _client(admin, consumer=None) -> KafkaAdminClient:
    client = KafkaAdminClient.__new__(KafkaAdminClient)
    client._admin = admin
    client._consumer = consumer
    client._timeout = 1.0
    return client


# ── list_topics ───────────────────────────────────────────────────────


def test_list_topics_drops_internal_topics():
    # __consumer_offsets is Kafka's own bookkeeping, not something anyone can act on
    admin = FakeAdmin(
        {"user-events": FakeTopicMetadata([0]), "__consumer_offsets": FakeTopicMetadata([0])}
    )
    assert _client(admin).list_topics() == ["user-events"]


def test_partitions_raises_when_topic_is_missing():
    with pytest.raises(RuntimeError, match="not available"):
        _client(FakeAdmin({})).partitions("nope")


def test_partitions_raises_when_metadata_carries_an_error():
    admin = FakeAdmin({TOPIC: FakeTopicMetadata([0], error=FakeError())})
    with pytest.raises(RuntimeError, match="not available"):
        _client(admin).partitions(TOPIC)


# ── consumer_group_lag ────────────────────────────────────────────────


def test_lag_sums_over_partitions():
    admin = FakeAdmin({TOPIC: FakeTopicMetadata([0, 1, 2])}, committed={0: 100, 1: 200, 2: 300})
    consumer = FakeConsumer({0: 150, 1: 200, 2: 500})
    lag = _client(admin, consumer).consumer_group_lag(GROUP, TOPIC)
    assert lag.total == 50 + 0 + 200
    assert lag.per_partition == {0: 50, 1: 0, 2: 200}
    assert lag.uncommitted_partitions == []


def test_uncommitted_partition_is_reported_not_counted_as_zero():
    # unknown lag folded in as 0 would understate the backlog exactly when it matters
    admin = FakeAdmin(
        {TOPIC: FakeTopicMetadata([0, 1])}, committed={0: 10, 1: OFFSET_INVALID}
    )
    consumer = FakeConsumer({0: 20, 1: 999})
    lag = _client(admin, consumer).consumer_group_lag(GROUP, TOPIC)
    assert lag.total == 10
    assert lag.uncommitted_partitions == [1]


def test_committed_ahead_of_watermark_does_not_produce_negative_lag():
    admin = FakeAdmin({TOPIC: FakeTopicMetadata([0])}, committed={0: 100})
    consumer = FakeConsumer({0: 90})
    assert _client(admin, consumer).consumer_group_lag(GROUP, TOPIC).total == 0


# ── consumer_group_state ──────────────────────────────────────────────


def test_stable_group_with_members_is_consuming():
    admin = FakeAdmin({}, group=FakeGroupDescription("STABLE", members=2))
    status = _client(admin).consumer_group_state(GROUP)
    assert (status.state, status.members) == ("STABLE", 2)
    assert status.is_consuming


def test_empty_group_is_not_consuming():
    # the earliest signal that a consumer died while the brokers stayed green
    admin = FakeAdmin({}, group=FakeGroupDescription("EMPTY", members=0))
    assert not _client(admin).consumer_group_state(GROUP).is_consuming


def test_stable_group_without_members_is_not_consuming():
    admin = FakeAdmin({}, group=FakeGroupDescription("STABLE", members=0))
    assert not _client(admin).consumer_group_state(GROUP).is_consuming
