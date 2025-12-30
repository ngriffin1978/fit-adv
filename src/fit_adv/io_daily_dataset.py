from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


DEFAULT_NUM_COLS: list[str] = [
    "recovery_score", "hrv_rmssd_milli", "resting_hr", "spo2_pct", "skin_temp_c",
    "sleep_perf_pct", "sleep_eff_pct", "sleep_consistency_pct", "resp_rate", "sleep_asleep_hours_est",
    "cycle_strain", "cycle_kilojoule", "cycle_avg_hr", "cycle_max_hr",
    "workout_count", "workout_minutes", "workout_strain_sum", "workout_kj_sum",
    "workout_avg_hr_mean", "workout_max_hr_max",
]

DEFAULT_WORKOUT_FILL_ZERO: list[str] = [
    "workout_count", "workout_minutes", "workout_strain_sum", "workout_kj_sum",
]


@dataclass(frozen=True)
class DailyDatasetLoadSpec:
    path: Path
    date_col: str = "date"
    numeric_cols: tuple[str, ...] = tuple(DEFAULT_NUM_COLS)
    workout_fill_zero_cols: tuple[str, ...] = tuple(DEFAULT_WORKOUT_FILL_ZERO)


def load_daily_dataset(
    spec: DailyDatasetLoadSpec,
    *,
    require_exists: bool = True,
) -> pd.DataFrame:
    """
    Load and preprocess the daily dataset CSV.

    - Validates file exists (optional)
    - Reads CSV into DataFrame
    - Parses and sorts by date
    - Coerces numeric columns
    - Fills workout fields with 0 (for stable aggregations/charts)
    """
    path = spec.path

    if require_exists and not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Generate it first (e.g., run 30_whoop_daily_dataset.ipynb)."
        )

    df = pd.read_csv(path)

    # Parse + sort date
    if spec.date_col in df.columns:
        df[spec.date_col] = pd.to_datetime(df[spec.date_col], errors="coerce")
        df = df.sort_values(spec.date_col).reset_index(drop=True)

    # Numeric coercion (if columns exist)
    for c in spec.numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Fill workout fields for stable charts/sums
    for c in spec.workout_fill_zero_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    return df

