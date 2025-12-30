# src/fit_adv/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _repo_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate repo root (pyproject.toml not found)")


@dataclass(frozen=True)
class Settings:
    # Base paths
    repo_root: Path = _repo_root()
    data_dir: Path = Path(os.getenv("FIT_ADV_DATA_DIR", "")) if os.getenv("FIT_ADV_DATA_DIR") else _repo_root() / "data"
    raw_dir: Path = Path(os.getenv("FIT_ADV_RAW_DIR", "")) if os.getenv("FIT_ADV_RAW_DIR") else _repo_root() / "data" / "raw"
    processed_dir: Path = Path(os.getenv("FIT_ADV_PROCESSED_DIR", "")) if os.getenv("FIT_ADV_PROCESSED_DIR") else _repo_root() / "data" / "processed"

    # Source/export CSV (your current input)
    source_csv: Path = Path(os.getenv("FIT_ADV_SOURCE_CSV", "")) if os.getenv("FIT_ADV_SOURCE_CSV") else _repo_root() / "data" / "whoop_export.csv"

    # Outputs (your normalized/derived outputs will go here)
    cycles_csv: Path = Path(os.getenv("FIT_ADV_CYCLES_CSV", "")) if os.getenv("FIT_ADV_CYCLES_CSV") else processed_dir / "cycles.csv"
    sleep_csv: Path = Path(os.getenv("FIT_ADV_SLEEP_CSV", "")) if os.getenv("FIT_ADV_SLEEP_CSV") else processed_dir / "sleep.csv"
    recovery_csv: Path = Path(os.getenv("FIT_ADV_RECOVERY_CSV", "")) if os.getenv("FIT_ADV_RECOVERY_CSV") else processed_dir / "recovery.csv"

    # Runtime
    timezone: str = os.getenv("FIT_ADV_TIMEZONE", "America/Chicago")


def get_settings() -> Settings:
    s = Settings()
    # Ensure local dirs exist (safe no-ops if already present)
    s.data_dir.mkdir(parents=True, exist_ok=True)
    s.raw_dir.mkdir(parents=True, exist_ok=True)
    s.processed_dir.mkdir(parents=True, exist_ok=True)
    return s

