from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


events = load_module("events", ROOT / "data-generator" / "events.py")
contracts = load_module("schema_contracts", ROOT / "data-quality" / "schema_contracts.py")


class EventFactoryTest(unittest.TestCase):
    def test_event_factory_generates_valid_events_for_every_topic(self):
        factory = events.EventFactory(seed=123)

        for topic in events.TOPICS:
            event = factory.next_event(topic)
            is_valid, errors = contracts.validate_event(topic, event)
            self.assertTrue(is_valid, f"{topic} event should be valid: {errors}")
            self.assertTrue(event["event_id"])
            self.assertTrue(event["event_ts"].endswith("Z"))

    def test_event_fingerprint_is_stable_for_dedupe_keys(self):
        event = {
            "order_id": "ORD-1",
            "product_id": "P1001",
            "event_ts": "2026-05-02T12:00:00Z",
            "ignored": "value-a",
        }
        changed_non_key = dict(event, ignored="value-b")

        first = contracts.event_fingerprint(event, ["order_id", "product_id", "event_ts"])
        second = contracts.event_fingerprint(changed_non_key, ["order_id", "product_id", "event_ts"])

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
