from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from fit_adv.config import Settings, get_settings
from fit_adv.io_raw_json import load_latest_raw_records
from fit_adv.io_records import records_to_frames
from fit_adv.build_daily_core import build_daily_from_cycle_recovery_sleep
from fit_adv.build_daily_dataset import add_workout_daily_metrics, write_daily_outputs, DailyBuildOutputs


@dataclass(frozen=True)
class DailyPipelineResult:
    outputs: DailyBuildOutputs
    df_daily: pd.DataFrame


def build_daily_from_latest_raw(settings: Settings | None = None) -> DailyPipelineResult:
    """
    End-to-end daily dataset build:
      latest raw JSON -> normalized frames -> df_daily -> add workouts -> write CSVs
    """
    s = settings or get_settings()

    recovery, sleep, workout, cycle = load_latest_raw_records(s.raw_dir)
    df_recovery, df_sleep, df_workout, df_cycle = records_to_frames(
        recovery=recovery, sleep=sleep, workout=workout, cycle=cycle
    )

    df_daily = build_daily_from_cycle_recovery_sleep(df_cycle, df_recovery, df_sleep)
    df_daily = add_workout_daily_metrics(df_daily, df_workout)

    outputs = write_daily_outputs(df_daily, out_dir=s.processed_dir)
    return DailyPipelineResult(outputs=outputs, df_daily=df_daily)

