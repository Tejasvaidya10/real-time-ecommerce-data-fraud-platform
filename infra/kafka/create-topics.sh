#!/usr/bin/env bash
set -euo pipefail

KAFKA_BIN=/opt/kafka/bin/kafka-topics.sh
BOOTSTRAP=kafka:19092

topics=(
  risk.decision.v1
  risk.alert.v1
  platform.dlq.v1
)

for topic in "${topics[@]}"; do
  "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions 6 --replication-factor 1
done

"$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --list
