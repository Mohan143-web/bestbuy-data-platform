#!/usr/bin/env bash
set -euo pipefail

echo "Starting Kafka and Kafka UI..."
docker compose up -d zookeeper kafka kafka-ui kafka-init

echo "Generating sample JSONL data for quick inspection..."
python3 data-generator/generate_events.py --topic all --count 10 --out-dir sample-data/events

echo "Start the full stream with:"
echo "  docker compose --profile stream up generator streaming-job"
echo
echo "Kafka UI: http://localhost:8080"
echo "Spark UI: http://localhost:8081"
