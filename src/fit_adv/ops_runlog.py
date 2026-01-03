from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_STATE_PATH = Path.home() / ".config" / "fit_adv" / "last_success.json"


def read_last_success(*, state_path: Path = DEFAULT_STATE_PATH) -> Optional[Dict[str, Any]]:
    if not state_path.exists():
        return None
    try:
        return json.loads(state_path.read_text())
    except Exception:
        return None


def write_last_success(service: str, summary: Dict[str, Any], *, state_path: Path = DEFAULT_STATE_PATH) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": service,
        "ts": int(time.time()),
        "summary": summary,
    }
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

