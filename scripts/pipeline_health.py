"""Emit simple pipeline health metrics from local lakehouse/checkpoint folders."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


def delta_table_metrics(path: Path) -> dict[str, int | str | bool]:
    delta_log = path / "_delta_log"
    commits = sorted(delta_log.glob("*.json")) if delta_log.exists() else []
    return {
        "path": str(path),
        "exists": path.exists(),
        "delta_commits": len(commits),
        "latest_commit": commits[-1].name if commits else "",
    }


def checkpoint_metrics(path: Path) -> dict[str, int | str | bool]:
    offsets = list(path.rglob("offsets/*")) if path.exists() else []
    commits = list(path.rglob("commits/*")) if path.exists() else []
    return {
        "path": str(path),
        "exists": path.exists(),
        "offset_files": len([item for item in offsets if item.is_file()]),
        "commit_files": len([item for item in commits if item.is_file()]),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse-root", default="lakehouse")
    parser.add_argument("--checkpoint-root", default="checkpoints")
    args = parser.parse_args()

    lakehouse_root = Path(args.lakehouse_root)
    checkpoint_root = Path(args.checkpoint_root)
    table_paths = [
        *sorted((lakehouse_root / "bronze").glob("*")),
        *sorted((lakehouse_root / "silver").glob("*")),
        *sorted((lakehouse_root / "gold").glob("*")),
        *sorted((lakehouse_root / "quarantine").glob("*")),
    ]
    checkpoint_paths = [path for path in checkpoint_root.glob("*/*") if path.is_dir()]

    payload = {
        "checked_at": datetime.now(UTC).isoformat(),
        "tables": [delta_table_metrics(path) for path in table_paths],
        "checkpoints": [checkpoint_metrics(path) for path in sorted(checkpoint_paths)],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

