from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from fit_adv.config import Settings, get_settings
from fit_adv.io_raw_json import load_latest_raw_records, load_all_raw_records
from fit_adv.io_records import records_to_frames
from fit_adv.build_daily_core import build_daily_from_cycle_recovery_sleep
from fit_adv.build_daily_dataset import (
    DailyBuildOutputs,
    add_workout_daily_metrics,
    write_daily_outputs,
)


@dataclass(frozen=True)
class DailyPipelineResult:
    outputs: DailyBuildOutputs
    df_daily: pd.DataFrame


def _filter_df_by_range(
    df: pd.DataFrame, *, start: str | None, end: str | None, ts_cols: list[str]
) -> pd.DataFrame:
    if df is None or df.empty or (start is None and end is None):
        return df

    # Find first usable timestamp column
    col = next((c for c in ts_cols if c in df.columns), None)
    if col is None:
        return df

    ts = pd.to_datetime(df[col], utc=True, errors="coerce")
    mask = ts.notna()
    if start is not None:
        start_ts = pd.to_datetime(start, utc=True)
        mask &= ts >= start_ts
    if end is not None:
        end_ts = pd.to_datetime(end, utc=True)
        mask &= ts < end_ts
    return df.loc[mask].copy()


def _collapse_daily_to_one_row_per_date(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the output is true daily grain (exactly one row per date).

    WHOOP cycle data can include multiple records that map to the same date; after
    joining recovery/sleep and adding workout aggregates, we collapse to one row
    per date deterministically.

    - Most fields are identical within a date => take first
    - Workout fields are true aggregates => sum
    """
    if df_daily is None or df_daily.empty or "date" not in df_daily.columns:
        return df_daily

    first_cols = [
        # Recovery
        "recovery_score",
        "hrv_rmssd_milli",
        "resting_hr",
        "spo2_pct",
        "skin_temp_c",
        "score_user_calibrating",
        # Sleep
        "sleep_perf_pct",
        "sleep_eff_pct",
        "sleep_consistency_pct",
        "resp_rate",
        "sleep_asleep_hours_est",
        # Cycle
        "cycle_strain",
        "cycle_kilojoule",
        "cycle_avg_hr",
        "cycle_max_hr",
    ]

    sum_cols = [
        "workout_count",
        "workout_strain_sum",
    ]

    agg: dict[str, str] = {}
    for c in first_cols:
        if c in df_daily.columns:
            agg[c] = "first"
    for c in sum_cols:
        if c in df_daily.columns:
            agg[c] = "sum"

    if not agg:
        return (
            df_daily.sort_values("date")
            .drop_duplicates("date", keep="first")
            .reset_index(drop=True)
        )

    out = (
        df_daily.sort_values("date")
        .groupby("date", as_index=False)
        .agg(agg)
    )
    return out


def _ensure_daily_min_schema(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df_daily always has at least a 'date' column so downstream writers
    never crash on empty builds or edge-case schema outcomes.
    """
    if df_daily is None:
        df_daily = pd.DataFrame()

    if "date" not in df_daily.columns:
        df_daily["date"] = pd.Series(dtype="string")

    return df_daily


def build_daily_from_latest_raw(settings: Settings | None = None) -> DailyPipelineResult:
    """
    End-to-end daily dataset build:
      latest raw JSON -> normalized frames -> df_daily -> add workouts -> collapse -> write CSVs
    """
    s = settings or get_settings()

    recovery, sleep, workout, cycle = load_latest_raw_records(s.raw_dir)
    df_recovery, df_sleep, df_workout, df_cycle = records_to_frames(
        recovery=recovery, sleep=sleep, workout=workout, cycle=cycle
    )

    df_daily = build_daily_from_cycle_recovery_sleep(df_cycle, df_recovery, df_sleep)
    df_daily = add_workout_daily_metrics(df_daily, df_workout)
    df_daily = _collapse_daily_to_one_row_per_date(df_daily)
    df_daily = _ensure_daily_min_schema(df_daily)

    outputs = write_daily_outputs(df_daily, out_dir=s.processed_dir)
    return DailyPipelineResult(outputs=outputs, df_daily=df_daily)


def build_daily_from_all_raw(
    *,
    start: str | None = None,
    end: str | None = None,
    settings: Settings | None = None,
) -> DailyPipelineResult:
    """
    Rebuild daily dataset from *all* raw JSON (optionally filtered by a time range).
    Intended for validating whoop-backfill and rebuilding historical periods.
    """
    s = settings or get_settings()

    recovery, sleep, workout, cycle = load_all_raw_records(s.raw_dir)
    df_recovery, df_sleep, df_workout, df_cycle = records_to_frames(
        recovery=recovery, sleep=sleep, workout=workout, cycle=cycle
    )

    # Optional range restriction (keeps rebuilds bounded)
    df_cycle = _filter_df_by_range(df_cycle, start=start, end=end, ts_cols=["start", "end"])
    df_sleep = _filter_df_by_range(df_sleep, start=start, end=end, ts_cols=["start", "end"])
    df_workout = _filter_df_by_range(df_workout, start=start, end=end, ts_cols=["start", "end"])
    df_recovery = _filter_df_by_range(
        df_recovery, start=start, end=end, ts_cols=["created_at", "updated_at", "timestamp"]
    )

    df_daily = build_daily_from_cycle_recovery_sleep(df_cycle, df_recovery, df_sleep)
    df_daily = add_workout_daily_metrics(df_daily, df_workout)
    df_daily = _collapse_daily_to_one_row_per_date(df_daily)
    df_daily = _ensure_daily_min_schema(df_daily)

    outputs = write_daily_outputs(df_daily, out_dir=s.processed_dir)
    return DailyPipelineResult(outputs=outputs, df_daily=df_daily)
