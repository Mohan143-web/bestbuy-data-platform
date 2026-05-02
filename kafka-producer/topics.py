"""Kafka topic bootstrap utilities."""

from __future__ import annotations

import argparse
import logging

from kafka.admin import KafkaAdminClient, NewTopic


TOPIC_CONFIG = {
    "orders": {"partitions": 3, "replication_factor": 1},
    "inventory": {"partitions": 3, "replication_factor": 1},
    "pricing": {"partitions": 2, "replication_factor": 1},
    "clickstream": {"partitions": 4, "replication_factor": 1},
}


def ensure_topics(bootstrap_server: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_server, client_id="bestbuy-topic-admin")
    existing = set(admin.list_topics())
    topics = [
        NewTopic(name=name, num_partitions=cfg["partitions"], replication_factor=cfg["replication_factor"])
        for name, cfg in TOPIC_CONFIG.items()
        if name not in existing
    ]
    if topics:
        admin.create_topics(topics)
        logging.info("Created Kafka topics: %s", ", ".join(topic.name for topic in topics))
    else:
        logging.info("Kafka topics already exist.")
    admin.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_topics(args.bootstrap_server)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

