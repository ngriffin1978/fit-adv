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
    # ---- Base paths ----
    repo_root: Path = _repo_root()

    data_dir: Path = (
        Path(os.getenv("FIT_ADV_DATA_DIR"))
        if os.getenv("FIT_ADV_DATA_DIR")
        else repo_root / "data"
    )

    raw_dir: Path = (
        Path(os.getenv("FIT_ADV_RAW_DIR"))
        if os.getenv("FIT_ADV_RAW_DIR")
        else data_dir / "raw"
    )

    processed_dir: Path = (
        Path(os.getenv("FIT_ADV_PROCESSED_DIR"))
        if os.getenv("FIT_ADV_PROCESSED_DIR")
        else data_dir / "processed"
    )

    # ---- Canonical processed outputs (ADD THESE HERE) ----
    daily_full_csv: Path = processed_dir / "daily_full.csv"
    daily_v1_csv: Path = processed_dir / "daily_v1.csv"

    # ---- Runtime ----
    timezone: str = os.getenv("FIT_ADV_TIMEZONE", "America/Chicago")


def get_settings() -> Settings:
    s = Settings()

    # Ensure directories exist
    s.data_dir.mkdir(parents=True, exist_ok=True)
    s.raw_dir.mkdir(parents=True, exist_ok=True)
    s.processed_dir.mkdir(parents=True, exist_ok=True)

    return s

