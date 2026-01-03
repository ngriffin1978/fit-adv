from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import duckdb


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DuckDbSink:
    db_path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self.db_path))
        con.execute("PRAGMA threads=4;")
        return con


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS run_log (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            ok BOOLEAN,
            params_json TEXT,
            notes TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_log (
            run_id TEXT,
            endpoint TEXT,
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            fetched_at TIMESTAMPTZ,
            status_code INTEGER,
            ok BOOLEAN,
            record_count BIGINT,
            raw_path TEXT,
            error TEXT
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_records (
            endpoint TEXT,
            record_id TEXT,
            updated_at TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ,
            payload_json TEXT
        );
        """
    )


def start_run(con: duckdb.DuckDBPyConnection, *, run_id: str, params: Dict[str, Any]) -> None:
    con.execute(
        "INSERT OR REPLACE INTO run_log(run_id, started_at, ok, params_json, notes) VALUES (?, ?, ?, ?, ?)",
        [run_id, utc_now_iso(), None, json.dumps(params), ""],
    )


def finish_run(con: duckdb.DuckDBPyConnection, *, run_id: str, ok: bool) -> None:
    con.execute(
        "UPDATE run_log SET finished_at = ?, ok = ? WHERE run_id = ?",
        [utc_now_iso(), ok, run_id],
    )


def log_fetch(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    endpoint: str,
    window_start: datetime,
    window_end: datetime,
    status_code: int,
    ok: bool,
    record_count: int,
    raw_path: Optional[Path],
    error: str = "",
) -> None:
    con.execute(
        """
        INSERT INTO fetch_log(
          run_id, endpoint, window_start, window_end, fetched_at,
          status_code, ok, record_count, raw_path, error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            endpoint,
            window_start.isoformat(),
            window_end.isoformat(),
            utc_now_iso(),
            int(status_code),
            bool(ok),
            int(record_count),
            str(raw_path) if raw_path else None,
            error or None,
        ],
    )


def ingest_records(
    con: duckdb.DuckDBPyConnection,
    *,
    endpoint: str,
    records: Iterable[Dict[str, Any]],
    record_id_field: str = "id",
    updated_at_field: Optional[str] = "updated_at",
    update_existing: bool = True,
) -> int:
    """
    Idempotent ingest into raw_records keyed by (endpoint, record_id).

    - Inserts new (endpoint, record_id) rows.
    - Optionally updates existing rows (payload_json/updated_at/ingested_at) when newer data arrives.
    """
    ingested_at = utc_now_iso()

    rows: list[tuple[str, str, Optional[str], str, str]] = []
    for r in records:
        rid = r.get(record_id_field)
        if rid is None:
            continue

        updated_at = None
        if updated_at_field:
            v = r.get(updated_at_field)
            if v:
                updated_at = v

        rows.append((endpoint, str(rid), updated_at, ingested_at, json.dumps(r)))

    if not rows:
        return 0

    # 1) Stage incoming rows (TEMP so it disappears at end of connection)
    con.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _raw_records_stage (
            endpoint     VARCHAR,
            record_id    VARCHAR,
            updated_at   TIMESTAMPTZ,
            ingested_at  TIMESTAMPTZ,
            payload_json VARCHAR
        );
        """
    )
    con.execute("DELETE FROM _raw_records_stage;")  # reuse temp table per call

    con.executemany(
        """
        INSERT INTO _raw_records_stage(endpoint, record_id, updated_at, ingested_at, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )

    # 2) Insert only records that don't already exist
    inserted = con.execute(
        """
        SELECT count(*)
        FROM _raw_records_stage s
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_records r
            WHERE r.endpoint = s.endpoint
              AND r.record_id = s.record_id
        );
        """
    ).fetchone()[0]

    con.execute(
        """
        INSERT INTO raw_records(endpoint, record_id, updated_at, ingested_at, payload_json)
        SELECT s.endpoint, s.record_id, s.updated_at, s.ingested_at, s.payload_json
        FROM _raw_records_stage s
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_records r
            WHERE r.endpoint = s.endpoint
              AND r.record_id = s.record_id
        );
        """
    )

    # 3) Optional: update existing rows if the staged row is newer
    if update_existing:
        con.execute(
            """
            UPDATE raw_records r
            SET
              updated_at   = COALESCE(s.updated_at, r.updated_at),
              ingested_at  = s.ingested_at,
              payload_json = s.payload_json
            FROM _raw_records_stage s
            WHERE r.endpoint = s.endpoint
              AND r.record_id = s.record_id
              AND (
                    r.updated_at IS NULL
                 OR s.updated_at IS NULL
                 OR s.updated_at >= r.updated_at
              );
            """
        )

    return int(inserted)



def ensure_latest_view(con: duckdb.DuckDBPyConnection, endpoint: str) -> None:
    con.execute(
        f"""
        CREATE VIEW IF NOT EXISTS {endpoint}_latest AS
        SELECT *
        FROM (
          SELECT
            endpoint,
            record_id,
            updated_at,
            ingested_at,
            payload_json,
            row_number() OVER (
              PARTITION BY endpoint, record_id
              ORDER BY
                CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END,
                updated_at DESC,
                ingested_at DESC
            ) AS rn
          FROM raw_records
          WHERE endpoint = '{endpoint}'
        )
        WHERE rn = 1;
        """
    )


def suspicious_empty_windows(con: duckdb.DuckDBPyConnection, *, run_id: str, min_hours: int = 6) -> int:
    return con.execute(
        f"""
        SELECT count(*)::INT
        FROM fetch_log
        WHERE run_id = ?
          AND ok = true
          AND record_count = 0
          AND (window_end - window_start) > INTERVAL '{int(min_hours)} hours'
        """,
        [run_id],
    ).fetchone()[0]
