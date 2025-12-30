from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from fit_adv.config import get_settings
from fit_adv.pipeline import build_daily_from_latest_raw


def _write_json(path: Path, payload: list[dict]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_daily_from_latest_raw_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    # Point the app at a temp data directory so we don't touch real repo data/
    monkeypatch.setenv("FIT_ADV_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("FIT_ADV_RAW_DIR", raising=False)
    monkeypatch.delenv("FIT_ADV_PROCESSED_DIR", raising=False)

    s = get_settings()  # creates data/raw and data/processed under tmp_path/data

    # Minimal WHOOP-like records
    cycle = [
        {
            "id": "c1",
            "start": "2025-12-01T06:00:00Z",
            "end": "2025-12-02T06:00:00Z",
            "timezone_offset": "-06:00",
            "score_state": "SCORED",
            "score": {
                "strain": 10.5,
                "kilojoule": 1200,
                "average_heart_rate": 130,
                "max_heart_rate": 175,
            },
        }
    ]

    recovery = [
        {
            "cycle_id": "c1",
            "score": {
                "recovery_score": 72,
                "hrv_rmssd_milli": 45.2,
                "resting_heart_rate": 55,
                "spo2_percentage": 97.5,
                "skin_temp_celsius": 36.6,
            },
        }
    ]

    sleep = [
        {
            "cycle_id": "c1",
            "start": "2025-12-01T23:00:00Z",
            "end": "2025-12-02T05:00:00Z",
            "nap": False,
            "score_state": "SCORED",
            "score": {
                "sleep_performance_percentage": 88,
                "sleep_efficiency_percentage": 92,
                "sleep_consistency_percentage": 80,
                "respiratory_rate": 15.5,
                "sleep_needed": {"baseline_milli": 28800000},
                "stage_summary": {
                    "total_in_bed_time_milli": 21600000,
                    "total_awake_time_milli": 1800000,
                    "total_light_sleep_time_milli": 9000000,
                    "total_slow_wave_sleep_time_milli": 4500000,
                    "total_rem_sleep_time_milli": 5400000,
                },
            },
        }
    ]

    workout = [
        {
            "id": "w1",
            "start": "2025-12-01T12:00:00Z",
            "end": "2025-12-01T12:45:00Z",
            "score": {
                "strain": 8.1,
                "kilojoule": 350,
                "average_heart_rate": 145,
                "max_heart_rate": 170,
            },
        }
    ]

    # Write latest raw JSON files your loader expects: <prefix>_*.json
    _write_json(s.raw_dir / "cycle_2025-12-02.json", cycle)
    _write_json(s.raw_dir / "recovery_2025-12-02.json", recovery)
    _write_json(s.raw_dir / "sleep_2025-12-02.json", sleep)
    _write_json(s.raw_dir / "workout_2025-12-02.json", workout)

    # Run pipeline
    result = build_daily_from_latest_raw(settings=s)

    # Assert: files exist
    assert result.outputs.daily_full_csv.exists()
    assert result.outputs.daily_v1_csv.exists()

    # Assert: key columns exist in v1 output
    df = pd.read_csv(result.outputs.daily_v1_csv)
    for col in ["date", "cycle_strain", "recovery_score", "sleep_perf_pct", "workout_count"]:
        assert col in df.columns

