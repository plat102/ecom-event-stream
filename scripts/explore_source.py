"""
One-off script: connect source Kafka, sample messages, print schema analysis.

Usage:
    poetry run python scripts/explore_source.py [--max-messages 20] [--timeout 30]

Output:
    - Schema table: field | type | null rate | sample value
    - Unknown fields (not in RawEvent model)
    - Required fields null check
    - 1 raw sample event
"""
import argparse
import json
from collections import defaultdict

from shared.config.settings import settings
from shared.connectors.kafka import make_source_consumer
from shared.schemas.event import REQUIRED_FIELDS, RawEvent

DOCUMENTED_FIELDS = {
    info.alias if info.alias else name
    for name, info in RawEvent.model_fields.items()
}


def collect_messages(max_messages: int, timeout_seconds: int) -> list[dict]:
    consumer = make_source_consumer(settings, group_id="tp-explore", offset_reset="earliest")
    consumer.subscribe([settings.SOURCE_KAFKA_TOPIC])

    messages = []
    empty_polls = 0

    print(f"Connecting to: {settings.SOURCE_KAFKA_BROKERS}")
    print(f"Topic: {settings.SOURCE_KAFKA_TOPIC}")
    print(f"Sampling {max_messages} messages (timeout {timeout_seconds}s)...\n")

    while len(messages) < max_messages and empty_polls < timeout_seconds:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            print(f"\r  Waiting... ({empty_polls}s)", end="", flush=True)
            continue
        if msg.error():
            print(f"\nKafka error: {msg.error()}")
            continue

        empty_polls = 0
        try:
            data = json.loads(msg.value())
            messages.append(data)
            print(f"\r  Collected {len(messages)}/{max_messages}", end="", flush=True)
        except json.JSONDecodeError as e:
            print(f"\nJSON parse error (offset {msg.offset()}): {e}")

    print()
    consumer.close()
    return messages


def analyze(messages: list[dict]) -> None:
    all_keys: set[str] = set()
    for m in messages:
        all_keys.update(m.keys())

    stats: dict[str, dict] = {}
    for key in sorted(all_keys):
        values = [m.get(key) for m in messages]
        non_null = [v for v in values if v is not None and v != ""]
        null_count = len(messages) - len(non_null)
        types = sorted({type(v).__name__ for v in non_null}) if non_null else ["null"]
        sample = non_null[0] if non_null else None
        stats[key] = {
            "types": "/".join(types),
            "null_count": null_count,
            "sample": sample,
        }

    w = 70
    print("=" * w)
    print(f"SCHEMA — {len(messages)} messages sampled from `{settings.SOURCE_KAFKA_TOPIC}`")
    print("=" * w)
    print(f"{'Field':<22} {'Type':<12} {'Nulls':<10} Sample")
    print("-" * w)

    for key, s in stats.items():
        marker = " (*)" if key in REQUIRED_FIELDS else "     "
        null_str = f"{s['null_count']}/{len(messages)}"
        sample_str = repr(s["sample"])[:28] if s["sample"] is not None else "—"
        print(f"{key + marker:<22} {s['types']:<12} {null_str:<10} {sample_str}")

    print(f"\n  (*) required field")

    # Unknown fields
    unknown = all_keys - DOCUMENTED_FIELDS
    if unknown:
        print(f"\n  WARN: undocumented fields → {sorted(unknown)}")
        print("        Add to RawEvent model if legitimate.")
    else:
        print(f"\n  OK: all fields match documented schema")

    # Required field null check
    issues = [
        f"  '{f}' — {stats[f]['null_count']} nulls" for f in REQUIRED_FIELDS
        if f in stats and stats[f]["null_count"] > 0
    ]
    never_seen = REQUIRED_FIELDS - all_keys
    if never_seen:
        print(f"\n  WARN: required fields never seen → {sorted(never_seen)}")
    if issues:
        print(f"\n  WARN: required fields with nulls →")
        print("\n".join(issues))

    # Raw sample
    print(f"\n{'=' * w}")
    print("RAW SAMPLE (1 event)")
    print("=" * w)
    print(json.dumps(messages[0], indent=2, default=str, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=30,
                        help="Stop after N seconds of no new messages")
    args = parser.parse_args()

    messages = collect_messages(args.max_messages, args.timeout)

    if not messages:
        print("No messages received. Check .env credentials and topic name.")
        return

    print(f"Collected {len(messages)} messages.\n")
    analyze(messages)


if __name__ == "__main__":
    main()
