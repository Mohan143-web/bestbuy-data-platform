"""Generate JSONL demo events for local smoke tests and README examples."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from events import EventFactory, TOPICS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce events.")
    parser.add_argument("--topic", choices=[*TOPICS, "all"], default="all")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", type=Path, help="Write JSONL files by topic instead of stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    factory = EventFactory(seed=args.seed)
    topics = TOPICS if args.topic == "all" else (args.topic,)

    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        for topic in topics:
            path = args.out_dir / f"{topic}.jsonl"
            with path.open("w", encoding="utf-8") as handle:
                for _ in range(args.count):
                    handle.write(json.dumps(factory.next_event(topic), sort_keys=True) + "\n")
            print(f"Wrote {args.count} {topic} events to {path}")
        return 0

    for idx in range(args.count):
        topic = topics[idx % len(topics)]
        print(json.dumps({"topic": topic, "value": factory.next_event(topic)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

