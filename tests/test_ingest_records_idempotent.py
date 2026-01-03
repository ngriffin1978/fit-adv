from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from fit_adv.io_duckdb import ingest_records


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def test_ingest_records_idempotent_inserts_only_once(tmp_path) -> None:
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    # Minimal schema consistent with your real table
    con.execute(
        """
        CREATE TABLE raw_records(
          endpoint     VARCHAR,
          record_id    VARCHAR,
          updated_at   TIMESTAMPTZ,
          ingested_at  TIMESTAMPTZ,
          payload_json VARCHAR
        );
        """
    )
    con.execute(
        "CREATE UNIQUE INDEX raw_records_uniq ON raw_records(endpoint, record_id);"
    )

    payload = {
        "id": "abc123",
        "updated_at": _utc_iso(datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        "foo": "bar",
    }

    # First ingest inserts 1
    n1 = ingest_records(con, endpoint="workout", records=[payload])
    assert n1 == 1
    assert con.execute("SELECT count(*) FROM raw_records").fetchone()[0] == 1

    # Second ingest of identical record inserts 0 (idempotent)
    n2 = ingest_records(con, endpoint="workout", records=[payload])
    assert n2 == 0
    assert con.execute("SELECT count(*) FROM raw_records").fetchone()[0] == 1

    # And the row is still the same record_id
    row = con.execute(
        "SELECT endpoint, record_id, payload_json FROM raw_records"
    ).fetchone()
    assert row[0] == "workout"
    assert row[1] == "abc123"
    assert json.loads(row[2])["foo"] == "bar"

    con.close()


def test_ingest_records_updates_when_newer_updated_at(tmp_path) -> None:
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    con.execute(
        """
        CREATE TABLE raw_records(
          endpoint     VARCHAR,
          record_id    VARCHAR,
          updated_at   TIMESTAMPTZ,
          ingested_at  TIMESTAMPTZ,
          payload_json VARCHAR
        );
        """
    )
    con.execute(
        "CREATE UNIQUE INDEX raw_records_uniq ON raw_records(endpoint, record_id);"
    )

    older = {
        "id": "abc123",
        "updated_at": _utc_iso(datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        "val": 1,
    }
    newer = {
        "id": "abc123",
        "updated_at": _utc_iso(datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        "val": 2,
    }

    ingest_records(con, endpoint="workout", records=[older])
    # Same record_id, newer updated_at should update payload_json (without inserting a 2nd row)
    n = ingest_records(con, endpoint="workout", records=[newer])
    assert n == 0
    assert con.execute("SELECT count(*) FROM raw_records").fetchone()[0] == 1

    payload_json = con.execute(
        "SELECT payload_json FROM raw_records WHERE endpoint='workout' AND record_id='abc123'"
    ).fetchone()[0]
    assert json.loads(payload_json)["val"] == 2

    con.close()

