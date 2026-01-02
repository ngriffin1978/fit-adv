from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


CORE_V1_COLUMNS: tuple[str, ...] = (
    "date",
    "recovery_score", "hrv_rmssd_milli", "resting_hr", "spo2_pct", "skin_temp_c",
    "sleep_perf_pct", "sleep_eff_pct", "sleep_consistency_pct", "resp_rate", "sleep_asleep_hours_est",
    "cycle_strain", "cycle_kilojoule", "cycle_avg_hr", "cycle_max_hr",
    "workout_count", "workout_minutes", "workout_strain_sum", "workout_kj_sum",
)


@dataclass(frozen=True)
class DailyBuildOutputs:
    out_dir: Path
    daily_full_csv: Path
    daily_v1_csv: Path


def add_workout_daily_metrics(df_daily: pd.DataFrame, df_workout: pd.DataFrame) -> pd.DataFrame:
    # Ensure output columns exist even when there are no workouts
    base_cols = {
        "workout_count": 0,
        "workout_strain_sum": 0.0,
        "workout_kilojoule_sum": 0.0,
        "workout_minutes_sum": 0.0,
    }
    for c, v in base_cols.items():
        if c not in df_daily.columns:
            df_daily[c] = v

    # Nothing to do if workouts are missing/empty
    if df_workout is None or df_workout.empty:
        return df_daily

    # Schema guard: if WHOOP changes or our normalize step didn't include fields
    required = {"start"}
    missing = required - set(df_workout.columns)
    if missing:
        # Keep zeros, don’t crash
        # (optional) print/log a warning here
        return df_daily

    w = df_workout.copy()

    # ---- existing logic below this point ----
    w["start_dt_utc"] = pd.to_datetime(w["start"], utc=True, errors="coerce")
    w["date"] = w["start_dt_utc"].dt.date.astype(str)

    # Example aggregations (keep your existing ones if different)
    agg = w.groupby("date").agg(
        workout_count=("start", "count"),
        workout_strain_sum=("strain", "sum") if "strain" in w.columns else ("start", "count"),
        workout_kilojoule_sum=("kilojoule", "sum") if "kilojoule" in w.columns else ("start", "count"),
        workout_minutes_sum=("duration_milli", lambda s: (s.fillna(0).sum() / 60000.0)) if "duration_milli" in w.columns else ("start", "count"),
    ).reset_index()

    df_daily = df_daily.merge(agg, on="date", how="left", suffixes=("", "_new"))

    # Fill merged values if present; keep zeros otherwise
    for c in base_cols.keys():
        if f"{c}_new" in df_daily.columns:
            df_daily[c] = df_daily[f"{c}_new"].fillna(df_daily[c])
            df_daily.drop(columns=[f"{c}_new"], inplace=True)

    return df_daily


def write_daily_outputs(
    df_daily: pd.DataFrame,
    out_dir: Path,
    *,
    daily_full_name: str = "daily_full.csv",
    daily_v1_name: str = "daily_v1.csv",
    float_format: str = "%.6f",
) -> DailyBuildOutputs:
    """
    Write full and v1 daily CSV outputs.
    - Sorts by date
    - Writes daily_full.csv
    - Writes daily_v1.csv with stable line endings and float formatting
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    df_sorted = df_daily.sort_values("date").reset_index(drop=True)

    daily_full_csv = out_dir / daily_full_name
    df_sorted.to_csv(daily_full_csv, index=False)

    # Build v1 subset
    cols = [c for c in CORE_V1_COLUMNS if c in df_sorted.columns]
    df_v1 = df_sorted[cols].copy()

    daily_v1_csv = out_dir / daily_v1_name
    df_v1.to_csv(
        daily_v1_csv,
        index=False,
        lineterminator="\n",
        float_format=float_format,
    )

    return DailyBuildOutputs(
        out_dir=out_dir,
        daily_full_csv=daily_full_csv,
        daily_v1_csv=daily_v1_csv,
    )

