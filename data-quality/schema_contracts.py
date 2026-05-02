"""Schema contracts and lightweight data-quality checks.

These pure-Python checks are used before publishing to Kafka and by tests. Spark
jobs apply the same contracts at dataframe level before writing Silver tables.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any


TYPE_MAP = {
    "string": str,
    "int": int,
    "float": (int, float),
}

EVENT_CONTRACTS: dict[str, dict[str, Any]] = {
    "orders": {
        "primary_key": ["event_id"],
        "dedupe_key": ["order_id", "product_id", "event_ts"],
        "event_time": "event_ts",
        "max_lateness_minutes": 120,
        "required": {
            "event_id": "string",
            "event_type": "string",
            "event_ts": "string",
            "order_id": "string",
            "user_id": "string",
            "product_id": "string",
            "store_id": "string",
            "quantity": "int",
            "unit_price": "float",
            "order_total": "float",
        },
    },
    "inventory": {
        "primary_key": ["event_id"],
        "dedupe_key": ["product_id", "store_id", "event_ts"],
        "event_time": "event_ts",
        "max_lateness_minutes": 180,
        "required": {
            "event_id": "string",
            "event_type": "string",
            "event_ts": "string",
            "product_id": "string",
            "store_id": "string",
            "on_hand": "int",
            "available_to_promise": "int",
            "reorder_point": "int",
        },
    },
    "pricing": {
        "primary_key": ["event_id"],
        "dedupe_key": ["product_id", "event_ts"],
        "event_time": "event_ts",
        "max_lateness_minutes": 240,
        "required": {
            "event_id": "string",
            "event_type": "string",
            "event_ts": "string",
            "product_id": "string",
            "old_price": "float",
            "new_price": "float",
        },
    },
    "clickstream": {
        "primary_key": ["event_id"],
        "dedupe_key": ["session_id", "event_type", "event_ts"],
        "event_time": "event_ts",
        "max_lateness_minutes": 60,
        "required": {
            "event_id": "string",
            "event_type": "string",
            "event_ts": "string",
            "session_id": "string",
            "user_id": "string",
            "page": "string",
            "device": "string",
        },
    },
}


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def validate_event(topic: str, event: dict[str, Any]) -> tuple[bool, list[str]]:
    contract = EVENT_CONTRACTS[topic]
    errors: list[str] = []

    for field, expected_type in contract["required"].items():
        value = event.get(field)
        if value is None:
            errors.append(f"{field}:null")
            continue
        if not isinstance(value, TYPE_MAP[expected_type]):
            errors.append(f"{field}:expected_{expected_type}")

    event_time = event.get(contract["event_time"])
    if event_time:
        try:
            parse_timestamp(event_time)
        except ValueError:
            errors.append(f"{contract['event_time']}:invalid_timestamp")

    return not errors, errors


def is_late(topic: str, event: dict[str, Any], now: datetime | None = None) -> bool:
    contract = EVENT_CONTRACTS[topic]
    if now is None:
        now = datetime.now(UTC)
    event_time = parse_timestamp(event[contract["event_time"]])
    age_minutes = (now - event_time).total_seconds() / 60
    return age_minutes > contract["max_lateness_minutes"]


def event_fingerprint(event: dict[str, Any], keys: list[str]) -> str:
    payload = {key: event.get(key) for key in keys}
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

