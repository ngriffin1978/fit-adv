from __future__ import annotations

from typing import Iterable, Mapping, Any
import pandas as pd


def flatten_score(records: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """
    Flatten WHOOP-style records that contain a nested 'score' dict.

    Input:
      [
        { ..., "score": {"a": 1, "b": 2}, ... },
        ...
      ]

    Output:
      DataFrame with columns:
        - original keys (except 'score')
        - score_<key> columns
    """
    rows = []

    for r in records:
        base = {k: v for k, v in r.items() if k != "score"}
        score = r.get("score") or {}

        for k, v in score.items():
            base[f"score_{k}"] = v

        rows.append(base)

    return pd.DataFrame(rows)


def records_to_frames(
    *,
    recovery: Iterable[Mapping[str, Any]],
    sleep: Iterable[Mapping[str, Any]],
    workout: Iterable[Mapping[str, Any]],
    cycle: Iterable[Mapping[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Convert raw WHOOP record lists into normalized DataFrames.

    Returns:
      (df_recovery, df_sleep, df_workout, df_cycle)
    """
    df_recovery = flatten_score(recovery)
    df_sleep = flatten_score(sleep)
    df_workout = flatten_score(workout)
    df_cycle = flatten_score(cycle)

    return df_recovery, df_sleep, df_workout, df_cycle

