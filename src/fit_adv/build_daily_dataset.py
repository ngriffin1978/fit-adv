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
    """
    Build per-day workout aggregates and merge them into df_daily on 'date'.
    Expects df_workout to have at least: start, end, id, score_strain, score_kilojoule,
    score_average_heart_rate, score_max_heart_rate.
    """
    w = df_workout.copy()

    w["start_dt_utc"] = pd.to_datetime(w["start"], utc=True, errors="coerce")
    w["end_dt_utc"] = pd.to_datetime(w["end"], utc=True, errors="coerce")
    w["date"] = w["start_dt_utc"].dt.date.astype(str)

    w["minutes"] = (w["end_dt_utc"] - w["start_dt_utc"]).dt.total_seconds() / 60.0

    workout_daily = w.groupby("date", as_index=False).agg(
        workout_count=("id", "count"),
        workout_minutes=("minutes", "sum"),
        workout_strain_sum=("score_strain", "sum"),
        workout_kj_sum=("score_kilojoule", "sum"),
        workout_avg_hr_mean=("score_average_heart_rate", "mean"),
        workout_max_hr_max=("score_max_heart_rate", "max"),
    )

    merged = df_daily.merge(workout_daily, on="date", how="left")
    return merged


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

