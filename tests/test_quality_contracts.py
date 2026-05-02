from __future__ import annotations

import importlib.util
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_contracts():
    spec = importlib.util.spec_from_file_location(
        "schema_contracts", ROOT / "data-quality" / "schema_contracts.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


contracts = load_contracts()


class QualityContractsTest(unittest.TestCase):
    def test_validate_event_reports_null_required_fields(self):
        event = {
            "event_id": "evt-1",
            "event_type": "order_created",
            "event_ts": "2026-05-02T12:00:00Z",
            "order_id": None,
            "user_id": "U1",
            "product_id": "P1001",
            "store_id": "SFO-001",
            "quantity": 1,
            "unit_price": 10.0,
            "order_total": 10.0,
        }

        is_valid, errors = contracts.validate_event("orders", event)

        self.assertFalse(is_valid)
        self.assertIn("order_id:null", errors)

    def test_late_event_detection_uses_topic_contract(self):
        now = datetime(2026, 5, 2, 12, 0, tzinfo=UTC)
        event = {
            "event_ts": (now - timedelta(minutes=181)).isoformat().replace("+00:00", "Z")
        }

        self.assertTrue(contracts.is_late("inventory", event, now=now))


if __name__ == "__main__":
    unittest.main()
