from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def write_raw_json(raw_dir: Path, prefix: str, records: List[Dict[str, Any]]) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{prefix}_{utc_stamp()}.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path

