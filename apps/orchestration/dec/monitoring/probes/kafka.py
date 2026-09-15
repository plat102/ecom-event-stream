"""What a Kafka check reads. Topics and groups arrive as arguments; nothing here reads config."""


def topics(client, *, cluster_name: str) -> dict:
    names = client.list_topics()
    return {"cluster": cluster_name, "topics": len(names), "names": sorted(names)}


def required_topics(client, *, expected: set[str]) -> dict:
    """Every expected topic at once: a missing DLQ is silent until an event needs rejecting."""
    present = set(client.list_topics())
    return {
        "expected": sorted(expected),
        "present": sorted(present),
        "missing": sorted(expected - present),
    }


def high_watermark_total(client, topic: str) -> int:
    """The counter throughput is derived from."""
    return sum(client.topic_high_watermark(topic).values())


def committed_total(client, group: str, topic: str) -> int:
    """The counter the processing rate is derived from."""
    offsets = client.committed_offsets(group, topic)
    return sum(offset for offset in offsets.values() if offset is not None)
