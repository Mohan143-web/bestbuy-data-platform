"""Continuously publish synthetic e-commerce events to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaProducer


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "data-generator"))
sys.path.insert(0, str(ROOT / "data-quality"))

from events import EventFactory, TOPICS  # noqa: E402
from schema_contracts import EVENT_CONTRACTS, event_fingerprint, validate_event  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish synthetic events to Kafka.")
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--rate-per-second", type=float, default=5)
    parser.add_argument("--max-events", type=int, default=0, help="0 means run forever.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--topic", choices=[*TOPICS, "all"], default="all")
    return parser.parse_args()


def build_producer(bootstrap_server: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=25,
        retries=5,
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    factory = EventFactory(seed=args.seed)
    producer = build_producer(args.bootstrap_server)
    topics = TOPICS if args.topic == "all" else (args.topic,)
    sleep_seconds = 1 / args.rate_per_second if args.rate_per_second > 0 else 0

    sent = 0
    logging.info("Publishing to %s topics on %s", ",".join(topics), args.bootstrap_server)
    try:
        while args.max_events == 0 or sent < args.max_events:
            topic = topics[sent % len(topics)]
            event = factory.next_event(topic)
            valid, errors = validate_event(topic, event)
            if not valid:
                logging.warning("Dropping invalid %s event: %s", topic, errors)
                continue

            key = event_fingerprint(event, EVENT_CONTRACTS[topic]["primary_key"])
            producer.send(topic, key=key, value=event)
            sent += 1

            if sent % 50 == 0:
                producer.flush()
                logging.info("Published %s events", sent)
            if sleep_seconds:
                time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        logging.info("Producer stopped after %s events", sent)
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
