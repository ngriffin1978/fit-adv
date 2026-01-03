from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fit_adv.ops_context import RunContext

DEFAULT_METRICS_DIR = Path.home() / ".config" / "fit_adv" / "metrics"


def _utc_compact(ts: Optional[float] = None) -> str:
    # e.g. 2026-01-02T19-07-07Z (filesystem-safe)
    dt = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H-%M-%SZ")


def write_run_metrics(
    ctx: RunContext,
    *,
    ok: bool,
    error: Optional[str] = None,
    metrics_dir: Path = DEFAULT_METRICS_DIR,
) -> Path:
    """
    Write one JSON metrics file per run (success or failure).
    Returns the written file path.
    """
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # Prefer started_at for deterministic naming
    ts_label = _utc_compact(ctx.started_at)
    suffix = "ok" if ok else "fail"
    out_path = metrics_dir / f"{ctx.service}_{ts_label}_{suffix}.json"

    payload = {
        "service": ctx.service,
        "ok": bool(ok),
        "error": error,
        "started_at_unix": ctx.started_at,
        "ended_at_unix": ctx.ended_at,
        "duration_s": round(ctx.duration_s, 3),
        "since": ctx.since,
        "since_hours": ctx.since_hours,
        "limit": ctx.limit,
        "endpoints": ctx.endpoints,
        "outputs": ctx.outputs,
        "extra": ctx.extra,
        "written_at_unix": time.time(),
    }

    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return out_path
