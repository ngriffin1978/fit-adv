from __future__ import annotations

import pandas as pd


def expand_dict_column(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
    """Expand a column containing dicts into top-level columns with a prefix."""
    if df is None or df.empty or col not in df.columns:
        return df
    expanded = df[col].apply(lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
    expanded = expanded.add_prefix(prefix)
    return pd.concat([df.drop(columns=[col]), expanded], axis=1)


def ms_to_hours(s: pd.Series) -> pd.Series:
    return s / 1000 / 60 / 60


def _coerce_cycle_id(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """
    Try to ensure df has a 'cycle_id' column.
    Returns: (df, has_cycle_id)
    """
    if df is None or df.empty:
        return df, False

    if "cycle_id" in df.columns:
        return df, True

    # Common alternates
    if "cycleId" in df.columns:
        df = df.rename(columns={"cycleId": "cycle_id"})
        return df, True

    if "cycle" in df.columns:
        # Sometimes "cycle" is an id, sometimes it's a dict/object with id
        if df["cycle"].apply(lambda x: isinstance(x, dict)).any():
            df["cycle_id"] = df["cycle"].apply(lambda x: x.get("id") if isinstance(x, dict) else None)
        else:
            df = df.rename(columns={"cycle": "cycle_id"})
        return df, "cycle_id" in df.columns

    # Nested-style columns from json_normalize, e.g. "cycle.id"
    if "cycle.id" in df.columns:
        df = df.rename(columns={"cycle.id": "cycle_id"})
        return df, True

    return df, False


def _ensure_date_from_timestamp(df: pd.DataFrame, *, preferred_ts_cols: list[str]) -> pd.DataFrame:
    """
    Ensure df has a 'date' column (YYYY-MM-DD string) by deriving from timestamp columns.
    """
    if df is None or df.empty:
        return df

    if "date" in df.columns:
        # Normalize to string for safe merges
        df["date"] = df["date"].astype(str)
        return df

    for ts_col in preferred_ts_cols:
        if ts_col in df.columns:
            dt = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
            if dt.notna().any():
                df["date"] = dt.dt.date.astype(str)
                return df

    return df


def build_daily_from_cycle_recovery_sleep(
    df_cycle: pd.DataFrame,
    df_recovery: pd.DataFrame,
    df_sleep: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build df_daily by:
      - deriving day from cycle start (UTC)
      - selecting/renaming cycle metrics
      - merging recovery on cycle_id (preferred), fallback to date join
      - expanding sleep nested dicts and merging one sleep per cycle (preferred), fallback to date join
      - deriving sleep_asleep_hours_est
    """
    # Normalize inputs
    c = df_cycle.copy() if df_cycle is not None else pd.DataFrame()
    r = df_recovery.copy() if df_recovery is not None else pd.DataFrame()
    s = df_sleep.copy() if df_sleep is not None else pd.DataFrame()

    # ---- Cycle -> df_daily base ----
    if c.empty:
        # If cycle is empty, we can't build the normal base table.
        # Return an empty df with expected columns (safe downstream).
        return pd.DataFrame()

    if "start" not in c.columns:
        # Without cycle.start we cannot derive date; fail soft with empty.
        return pd.DataFrame()

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

    # Ensure cycle_id as string
    if "cycle_id" in df_daily.columns:
        df_daily["cycle_id"] = df_daily["cycle_id"].astype(str)

    # ---- Join recovery ----
    # Rename key recovery metrics first (safe even if empty)
    r = r.rename(columns={
        "score_recovery_score": "recovery_score",
        "score_hrv_rmssd_milli": "hrv_rmssd_milli",
        "score_resting_heart_rate": "resting_hr",
        "score_spo2_percentage": "spo2_pct",
        "score_skin_temp_celsius": "skin_temp_c",
    })

    # Try to coerce/derive cycle_id
    r, has_r_cycle_id = _coerce_cycle_id(r)
    if has_r_cycle_id:
        r["cycle_id"] = r["cycle_id"].astype(str)

    recovery_keep = [
        "cycle_id",
        "recovery_score",
        "hrv_rmssd_milli",
        "resting_hr",
        "spo2_pct",
        "skin_temp_c",
        "score_user_calibrating",
    ]

    if not r.empty and has_r_cycle_id and "cycle_id" in df_daily.columns:
        df_daily = df_daily.merge(
            r[[col for col in recovery_keep if col in r.columns]],
            on="cycle_id",
            how="left",
        )
    else:
        # Fallback: merge recovery by date (less precise but keeps pipeline running)
        r = _ensure_date_from_timestamp(
            r,
            preferred_ts_cols=["created_at", "updated_at", "timestamp", "start", "end"],
        )
        if not r.empty and "date" in r.columns and "date" in df_daily.columns:
            # One recovery per date; if multiple, keep the most recent timestamp-like row
            if "created_at" in r.columns:
                r["_dt"] = pd.to_datetime(r["created_at"], utc=True, errors="coerce")
                r_sorted = r.sort_values(["date", "_dt"], ascending=[True, False])
                r_one = r_sorted.drop_duplicates("date", keep="first").drop(columns=["_dt"], errors="ignore")
            else:
                r_one = r.drop_duplicates("date", keep="first")

            recovery_keep_date = [c for c in recovery_keep if c != "cycle_id"]
            keep_cols = ["date"] + [col for col in recovery_keep_date if col in r_one.columns]
            df_daily = df_daily.merge(r_one[keep_cols], on="date", how="left")

    # ---- Join sleep ----
    # Try to coerce/derive cycle_id for sleep
    s, has_s_cycle_id = _coerce_cycle_id(s)
    if has_s_cycle_id:
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

    if not s.empty and has_s_cycle_id and "cycle_id" in df_daily.columns and "start" in s.columns:
        # Prefer the main sleep (nap == False). For ties, keep the latest start.
        s["start_dt"] = pd.to_datetime(s["start"], utc=True, errors="coerce")
        # nap False should win => sort nap ascending (False < True)
        if "nap" not in s.columns:
            s["nap"] = False
        s_sorted = s.sort_values(["cycle_id", "nap", "start_dt"], ascending=[True, True, False])
        s_one = s_sorted.drop_duplicates("cycle_id", keep="first")

        df_daily = df_daily.merge(
            s_one[[col for col in sleep_keep if col in s_one.columns]],
            on="cycle_id",
            how="left",
        )
    else:
        # Fallback: merge sleep by date derived from sleep.start (or others)
        s = _ensure_date_from_timestamp(
            s,
            preferred_ts_cols=["start", "end", "created_at", "updated_at", "timestamp"],
        )
        if not s.empty and "date" in s.columns and "date" in df_daily.columns:
            if "start" in s.columns:
                s["_dt"] = pd.to_datetime(s["start"], utc=True, errors="coerce")
                if "nap" not in s.columns:
                    s["nap"] = False
                s_sorted = s.sort_values(["date", "nap", "_dt"], ascending=[True, True, False])
                s_one = s_sorted.drop_duplicates("date", keep="first").drop(columns=["_dt"], errors="ignore")
            else:
                s_one = s.drop_duplicates("date", keep="first")

            sleep_keep_date = [c for c in sleep_keep if c != "cycle_id"]
            keep_cols = ["date"] + [col for col in sleep_keep_date if col in s_one.columns]
            df_daily = df_daily.merge(s_one[keep_cols], on="date", how="left")

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

