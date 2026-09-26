#!/bin/bash
# Creates the project's Kafka topics. Runs once as the kafka-init service; safe to re-run.
# Owner: Member A (Kafka lead). Partition counts are justified in docs/decisions.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:9092}"
TOPICS_BIN=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  local name="$1" partitions="$2"
  echo "{\"component\":\"kafka-init\",\"event\":\"create_topic\",\"topic\":\"${name}\",\"partitions\":${partitions}}"
  "$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1
}

create_topic "${VITALS_TOPIC:-patient-vitals}"  "${VITALS_PARTITIONS:-3}"
create_topic "${DLQ_TOPIC:-vitals-dlq}"         "${DLQ_PARTITIONS:-1}"
create_topic "${ALERTS_TOPIC:-patient-alerts}"  "${ALERTS_PARTITIONS:-3}"

"$TOPICS_BIN" --bootstrap-server "$BOOTSTRAP" --describe
