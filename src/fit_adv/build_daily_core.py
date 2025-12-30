from __future__ import annotations

import pandas as pd


def expand_dict_column(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
    """Expand a column containing dicts into top-level columns with a prefix."""
    if col not in df.columns:
        return df
    expanded = df[col].apply(lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
    expanded = expanded.add_prefix(prefix)
    return pd.concat([df.drop(columns=[col]), expanded], axis=1)


def ms_to_hours(s: pd.Series) -> pd.Series:
    return s / 1000 / 60 / 60


def build_daily_from_cycle_recovery_sleep(
    df_cycle: pd.DataFrame,
    df_recovery: pd.DataFrame,
    df_sleep: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build df_daily by:
      - deriving day from cycle start (UTC)
      - selecting/renaming cycle metrics
      - merging recovery on cycle_id
      - expanding sleep nested dicts and merging one sleep per cycle
      - deriving sleep_asleep_hours_est
    """
    # ---- Cycle -> df_daily base ----
    c = df_cycle.copy()
    c["start_dt_utc"] = pd.to_datetime(c["start"], utc=True, errors="coerce")
    c["date"] = c["start_dt_utc"].dt.date.astype(str)

    cycle_keep = [
        "id", "date", "start", "end", "timezone_offset", "score_state",
        "score_strain", "score_kilojoule", "score_average_heart_rate", "score_max_heart_rate"
    ]
    df_daily = c[[col for col in cycle_keep if col in c.columns]].copy()
    df_daily = df_daily.rename(columns={
        "id": "cycle_id",
        "score_strain": "cycle_strain",
        "score_kilojoule": "cycle_kilojoule",
        "score_average_heart_rate": "cycle_avg_hr",
        "score_max_heart_rate": "cycle_max_hr",
    })

    # ---- Join recovery ----
    df_daily["cycle_id"] = df_daily["cycle_id"].astype(str)

    r = df_recovery.copy()
    r["cycle_id"] = r["cycle_id"].astype(str)

    r = r.rename(columns={
        "score_recovery_score": "recovery_score",
        "score_hrv_rmssd_milli": "hrv_rmssd_milli",
        "score_resting_heart_rate": "resting_hr",
        "score_spo2_percentage": "spo2_pct",
        "score_skin_temp_celsius": "skin_temp_c",
    })

    recovery_keep = [
        "cycle_id",
        "recovery_score",
        "hrv_rmssd_milli",
        "resting_hr",
        "spo2_pct",
        "skin_temp_c",
        "score_user_calibrating",
    ]

    df_daily = df_daily.merge(
        r[[col for col in recovery_keep if col in r.columns]],
        on="cycle_id",
        how="left",
    )

    # ---- Join sleep ----
    s = df_sleep.copy()
    s["cycle_id"] = s["cycle_id"].astype(str)

    # Expand nested dict columns from score
    s = expand_dict_column(s, "score_sleep_needed", "sleep_needed_")
    s = expand_dict_column(s, "score_stage_summary", "stage_")

    # Rename key sleep metrics
    s = s.rename(columns={
        "score_sleep_performance_percentage": "sleep_perf_pct",
        "score_sleep_efficiency_percentage": "sleep_eff_pct",
        "score_sleep_consistency_percentage": "sleep_consistency_pct",
        "score_respiratory_rate": "resp_rate",
    })

    sleep_keep = (
        ["cycle_id", "start", "end", "nap", "score_state",
         "sleep_perf_pct", "sleep_eff_pct", "sleep_consistency_pct", "resp_rate"]
        + [col for col in s.columns if col.startswith("sleep_needed_") or col.startswith("stage_")]
    )

    # Prefer the main sleep (nap == False). For ties, keep the latest start.
    s["start_dt"] = pd.to_datetime(s["start"], utc=True, errors="coerce")
    s_sorted = s.sort_values(["cycle_id", "nap", "start_dt"], ascending=[True, True, False])
    s_one = s_sorted.drop_duplicates("cycle_id", keep="first")

    df_daily = df_daily.merge(
        s_one[[col for col in sleep_keep if col in s_one.columns]],
        on="cycle_id",
        how="left",
    )

    # Rename merge suffixes (cycle start/end vs sleep start/end)
    df_daily = df_daily.rename(columns={
        "start_x": "cycle_start",
        "end_x": "cycle_end",
        "score_state_x": "cycle_score_state",
        "start_y": "sleep_start",
        "end_y": "sleep_end",
        "score_state_y": "sleep_score_state",
    })

    # Convert sleep stage millis → hours AFTER merge
    for col in [
        "stage_total_in_bed_time_milli",
        "stage_total_awake_time_milli",
        "stage_total_light_sleep_time_milli",
        "stage_total_slow_wave_sleep_time_milli",
        "stage_total_rem_sleep_time_milli",
    ]:
        if col in df_daily.columns:
            df_daily[col.replace("_milli", "_hours")] = ms_to_hours(df_daily[col])

    if "stage_total_in_bed_time_hours" in df_daily.columns and "stage_total_awake_time_hours" in df_daily.columns:
        df_daily["sleep_asleep_hours_est"] = (
            df_daily["stage_total_in_bed_time_hours"] - df_daily["stage_total_awake_time_hours"]
        )

    return df_daily

