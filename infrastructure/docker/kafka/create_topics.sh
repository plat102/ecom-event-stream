#!/bin/bash
# Create Kafka topics — idempotent (safe to re-run)
# Usage: docker exec broker bash < infrastructure/docker/create_topics.sh
set -e

BROKER="kafka-0:29092"

kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic user-events --partitions 3 --replication-factor 3

kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic user-events-dlq --partitions 1 --replication-factor 3

echo "Topics:"
kafka-topics --bootstrap-server "$BROKER" --list
