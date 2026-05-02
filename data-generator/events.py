"""Synthetic Best Buy-style event generation.

The generator intentionally produces realistic-but-fake operational data:
orders, inventory changes, price updates, and clickstream activity.
"""

from __future__ import annotations

import csv
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = ROOT / "sample-data" / "product_catalog.csv"

TOPICS = ("orders", "inventory", "pricing", "clickstream")
STORES = ("SFO-001", "SEA-002", "AUS-003", "NYC-004", "ATL-005")
CHANNELS = ("web", "mobile_app", "store_pickup", "marketplace")
CLICK_EVENTS = ("page_view", "product_view", "search", "add_to_cart", "checkout_start")
DEVICES = ("ios", "android", "desktop", "tablet")


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def load_catalog(path: Path = DEFAULT_CATALOG_PATH) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    for row in rows:
        row["base_price"] = float(row["base_price"])
        row["cost"] = float(row["cost"])
        row["reorder_point"] = int(row["reorder_point"])
    return rows


class EventFactory:
    """Stateful event factory with deterministic seeding for tests and demos."""

    def __init__(self, seed: int | None = None, catalog_path: Path = DEFAULT_CATALOG_PATH) -> None:
        self.random = random.Random(seed)
        self.catalog = load_catalog(catalog_path)
        self.inventory_state = {
            (product["product_id"], store): self.random.randint(5, 80)
            for product in self.catalog
            for store in STORES
        }

    def _event_time(self, late_probability: float = 0.08) -> datetime:
        now = utc_now()
        if self.random.random() < late_probability:
            return now - timedelta(minutes=self.random.randint(30, 180))
        return now - timedelta(seconds=self.random.randint(0, 30))

    def _product(self) -> dict[str, Any]:
        return self.random.choice(self.catalog)

    def order(self) -> dict[str, Any]:
        product = self._product()
        quantity = self.random.choices([1, 2, 3, 4], weights=[72, 20, 6, 2], k=1)[0]
        unit_price = round(product["base_price"] * self.random.uniform(0.88, 1.08), 2)
        event_ts = self._event_time()
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_created",
            "event_ts": isoformat_z(event_ts),
            "ingestion_ts": isoformat_z(utc_now()),
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "user_id": f"U{self.random.randint(100000, 999999)}",
            "product_id": product["product_id"],
            "sku": product["sku"],
            "store_id": self.random.choice(STORES),
            "channel": self.random.choice(CHANNELS),
            "quantity": quantity,
            "unit_price": unit_price,
            "order_total": round(quantity * unit_price, 2),
            "currency": "USD",
            "status": self.random.choices(
                ["created", "paid", "cancelled"], weights=[68, 28, 4], k=1
            )[0],
        }

    def inventory_update(self) -> dict[str, Any]:
        product = self._product()
        store_id = self.random.choice(STORES)
        key = (product["product_id"], store_id)
        delta = self.random.randint(-8, 15)
        self.inventory_state[key] = max(0, self.inventory_state[key] + delta)
        on_hand = self.inventory_state[key]
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "inventory_update",
            "event_ts": isoformat_z(self._event_time()),
            "ingestion_ts": isoformat_z(utc_now()),
            "product_id": product["product_id"],
            "sku": product["sku"],
            "store_id": store_id,
            "on_hand": on_hand,
            "available_to_promise": max(0, on_hand - self.random.randint(0, 3)),
            "reorder_point": product["reorder_point"],
            "change_quantity": delta,
            "reason": self.random.choice(["sale", "return", "cycle_count", "replenishment"]),
        }

    def price_change(self) -> dict[str, Any]:
        product = self._product()
        old_price = round(product["base_price"] * self.random.uniform(0.9, 1.05), 2)
        new_price = round(old_price * self.random.uniform(0.92, 1.12), 2)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "price_change",
            "event_ts": isoformat_z(self._event_time()),
            "ingestion_ts": isoformat_z(utc_now()),
            "product_id": product["product_id"],
            "sku": product["sku"],
            "old_price": old_price,
            "new_price": new_price,
            "currency": "USD",
            "reason": self.random.choice(["promotion", "markdown", "competitor_match", "reset"]),
        }

    def clickstream(self) -> dict[str, Any]:
        product = self._product()
        click_event = self.random.choice(CLICK_EVENTS)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": click_event,
            "event_ts": isoformat_z(self._event_time()),
            "ingestion_ts": isoformat_z(utc_now()),
            "session_id": f"S{uuid.uuid4().hex[:16]}",
            "user_id": f"U{self.random.randint(100000, 999999)}",
            "product_id": product["product_id"] if click_event != "search" else None,
            "sku": product["sku"] if click_event != "search" else None,
            "page": self.random.choice(["home", "search", "product_detail", "cart", "checkout"]),
            "search_term": self.random.choice(["tv", "laptop", "headphones", "console", None]),
            "device": self.random.choice(DEVICES),
            "referrer": self.random.choice(["organic", "email", "paid_search", "direct"]),
        }

    def next_event(self, topic: str) -> dict[str, Any]:
        if topic == "orders":
            return self.order()
        if topic == "inventory":
            return self.inventory_update()
        if topic == "pricing":
            return self.price_change()
        if topic == "clickstream":
            return self.clickstream()
        raise ValueError(f"Unknown topic: {topic}")

