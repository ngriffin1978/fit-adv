from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from typing import Iterable

@dataclass(frozen=True)
class RawWhoopPaths:
    recovery: Path
    sleep: Path
    workout: Path
    cycle: Path

def _all_files(raw_dir: Path, prefix: str) -> list[Path]:
    files = sorted(raw_dir.glob(f"{prefix}_*.json"))
    if not files:
        raise FileNotFoundError(f"No {prefix}_*.json files found in {raw_dir}")
    return files


def load_all_raw_records(raw_dir: Path) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """
    Load *all* raw records from raw_dir by concatenating all prefix_*.json files.
    Returns: (recovery, sleep, workout, cycle)
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"raw_dir not found: {raw_dir}")

    def load(prefix: str) -> list[Mapping[str, Any]]:
        out: list[Mapping[str, Any]] = []
        for p in _all_files(raw_dir, prefix):
            out.extend(read_json_list(p))
        return out

    recovery = load("recovery")
    sleep = load("sleep")
    workout = load("workout")
    cycle = load("cycle")
    return recovery, sleep, workout, cycle

def _latest_file(raw_dir: Path, prefix: str) -> Path:
    files = sorted(raw_dir.glob(f"{prefix}_*.json"))
    if not files:
        raise FileNotFoundError(f"No {prefix}_*.json files found in {raw_dir}")
    return files[-1]


def find_latest_raw_paths(raw_dir: Path) -> RawWhoopPaths:
    """
    Find latest raw JSON files by prefix in a directory:
      recovery_*.json, sleep_*.json, workout_*.json, cycle_*.json
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"raw_dir not found: {raw_dir}")

    return RawWhoopPaths(
        recovery=_latest_file(raw_dir, "recovery"),
        sleep=_latest_file(raw_dir, "sleep"),
        workout=_latest_file(raw_dir, "workout"),
        cycle=_latest_file(raw_dir, "cycle"),
    )


def read_json_list(path: Path) -> list[Mapping[str, Any]]:
    """
    Read a JSON file containing a top-level list of dict-like objects.
    """
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list in {path}, got {type(data)}")
    return data  # type: ignore[return-value]


def load_latest_raw_records(raw_dir: Path) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """
    Load latest raw records from raw_dir.
    Returns: (recovery, sleep, workout, cycle)
    """
    paths = find_latest_raw_paths(raw_dir)
    recovery = read_json_list(paths.recovery)
    sleep = read_json_list(paths.sleep)
    workout = read_json_list(paths.workout)
    cycle = read_json_list(paths.cycle)
    return recovery, sleep, workout, cycle

