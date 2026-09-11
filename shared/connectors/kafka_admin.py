"""Read-only Kafka admin surface: topics, consumer group state, and raw offsets.
Offsets stay raw — holding the earlier observation a rate needs is not this client's job.
"""
from dataclasses import dataclass, field
from typing import Self

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

# librdkafka's sentinel for "this partition has no committed offset".
OFFSET_INVALID = -1001


@dataclass(frozen=True)
class ConsumerGroupStatus:
    group: str
    state: str
    members: int

    @property
    def is_consuming(self) -> bool:
        """A group with no members is not consuming, whatever the brokers report."""
        return self.state == "STABLE" and self.members > 0


@dataclass(frozen=True)
class ConsumerGroupLag:
    group: str
    topic: str
    total: int
    per_partition: dict[int, int] = field(default_factory=dict)
    # Partitions the group has never committed on — excluded from `total`, because their
    # lag is unknown rather than zero.
    uncommitted_partitions: list[int] = field(default_factory=list)


class KafkaAdminClient:
    """`AdminClient` plus a non-subscribing `Consumer` — watermarks live on the consumer API."""

    def __init__(self, config: dict, timeout: float = 10.0) -> None:
        self._timeout = timeout
        self._config = {k: v for k, v in config.items() if k != "group.id"}
        self._admin = AdminClient(self._config)
        # Mandatory for a Consumer; this one never subscribes, so no group is created.
        self._consumer = Consumer(
            {
                **self._config,
                "group.id": config.get("group.id", "airflow-monitor"),
                "enable.auto.commit": "false",
            }
        )

    def list_topics(self) -> list[str]:
        """Round-trips to a broker, so it doubles as the connectivity check; internal topics
        are dropped."""
        metadata = self._admin.list_topics(timeout=self._timeout)
        return sorted(name for name in metadata.topics if not name.startswith("__"))

    def partitions(self, topic: str) -> list[int]:
        metadata = self._admin.list_topics(topic=topic, timeout=self._timeout)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            error = topic_metadata and topic_metadata.error
            raise RuntimeError(f"topic {topic!r} not available: {error}")
        return sorted(topic_metadata.partitions)

    def topic_high_watermark(self, topic: str) -> dict[int, int]:
        """High watermark per partition — the offset the next produced message will get.

        One blocking call per partition, so the timeout is shared out between them: the whole
        read has to stay inside the caller's own timeout, however many partitions there are.
        """
        partitions = self.partitions(topic)
        per_partition = max(self._timeout / max(len(partitions), 1), 1.0)
        watermarks = {}
        for partition in partitions:
            _, high = self._consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=per_partition, cached=False
            )
            watermarks[partition] = high
        return watermarks

    def committed_offsets(self, group: str, topic: str) -> dict[int, int | None]:
        """Committed offset per partition; `None` where the group has committed nothing."""
        request = ConsumerGroupTopicPartitions(
            group, [TopicPartition(topic, p) for p in self.partitions(topic)]
        )
        futures = self._admin.list_consumer_group_offsets([request])
        result = futures[group].result(timeout=self._timeout)
        return {
            tp.partition: (None if tp.offset == OFFSET_INVALID else tp.offset)
            for tp in result.topic_partitions
        }

    def consumer_group_lag(self, group: str, topic: str) -> ConsumerGroupLag:
        """Committed offset against high watermark, summed over partitions."""
        watermarks = self.topic_high_watermark(topic)
        committed = self.committed_offsets(group, topic)
        per_partition, uncommitted = {}, []
        for partition, high in watermarks.items():
            offset = committed.get(partition)
            if offset is None:
                uncommitted.append(partition)
                continue
            per_partition[partition] = max(high - offset, 0)
        return ConsumerGroupLag(
            group=group,
            topic=topic,
            total=sum(per_partition.values()),
            per_partition=per_partition,
            uncommitted_partitions=sorted(uncommitted),
        )

    def consumer_group_state(self, group: str) -> ConsumerGroupStatus:
        """A group that never existed reports DEAD rather than raising."""
        futures = self._admin.describe_consumer_groups([group])
        description = futures[group].result(timeout=self._timeout)
        return ConsumerGroupStatus(
            group=group,
            state=description.state.name,
            members=len(description.members),
        )

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.close()
