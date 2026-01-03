# src/fit_adv/persist.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from fit_adv.io_raw_writer import write_raw_json
from fit_adv.io_duckdb import (
    DuckDbSink,
    init_schema,
    log_fetch,
    ingest_records,
    latest_view_sql,
)

import duckdb


@dataclass(frozen=True)
class PersistConfig:
    raw_dir: Path
    db_path: Path


@dataclass(frozen=True)
class PersistHandles:
    con: duckdb.DuckDBPyConnection


def open_persist(cfg: PersistConfig) -> PersistHandles:
    sink = DuckDbSink(cfg.db_path)
    con = sink.connect()
    init_schema(con)

    # Create latest views once (optional now, useful later)
    for ep in ("cycle", "sleep", "recovery", "workout"):
        con.execute(latest_view_sql(ep))

    return PersistHandles(con=con)


def close_persist(h: PersistHandles) -> None:
    h.con.close()


def persist_window(
    *,
    h: PersistHandles,
    run_id: str,
    endpoint: str,
    window_start: datetime,
    window_end: datetime,
    status_code: int,
    ok: bool,
    records: list[Dict[str, Any]],
    raw_prefix: str,
    raw_dir: Path,
    record_id_field: str,
    updated_at_field: Optional[str] = None,
    error: str = "",
) -> Path:
    # 1) Write raw JSON (atomic)
    raw_path = write_raw_json(
        raw_dir,
        raw_prefix,
        records,
        meta={
            "endpoint": endpoint,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "status_code": status_code,
            "ok": ok,
            "record_count": len(records),
            "error": error,
        },
    )

    # 2) Log fetch in DuckDB
    log_fetch(
        h.con,
        run_id=run_id,
        endpoint=endpoint,
        window_start=window_start,
        window_end=window_end,
        status_code=status_code,
        ok=ok,
        record_count=len(records),
        raw_path=raw_path,
        error=error,
    )

    # 3) Ingest records into DuckDB (append-only)
    if ok and records:
        ingest_records(
            h.con,
            endpoint=endpoint,
            records=records,
            record_id_field=record_id_field,
            updated_at_field=updated_at_field,
        )

    return raw_path
