#!/usr/bin/env bash
set -euo pipefail

TOPIC_NAME="${1:-orders}"
PARTITIONS="${2:-6}"
REPLICATION_FACTOR="${3:-2}"
BOOTSTRAP_SERVERS="${4:-kafka-1:9092,kafka-2:9092}"

# Execute inside kafka-1 so broker DNS names are reachable.
docker exec kafka-1 kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC_NAME}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}"

docker exec kafka-1 kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --describe \
  --topic "${TOPIC_NAME}"
