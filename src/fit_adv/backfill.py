from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Tuple


def _parse_isoish_utc(s: str) -> datetime:
    """
    Accepts:
      - full ISO timestamp (with or without tz)
      - YYYY-MM-DD (treated as UTC midnight)
    Returns timezone-aware UTC datetime.
    """
    s = s.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        # YYYY-MM-DD
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc)

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso(dt: datetime) -> str:
    # WHOOP accepts ISO-8601; we’ll always send UTC with offset.
    return dt.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    @property
    def start_iso(self) -> str:
        return _to_iso(self.start)

    @property
    def end_iso(self) -> str:
        return _to_iso(self.end)


def compute_backfill_range(
    *,
    since: Optional[str],
    days: Optional[int],
    until: Optional[str],
    now_utc: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """
    Determine [start, end) in UTC.
    - If since provided: start = since
    - Else if days provided: start = now - days
    - end = until if provided else now
    """
    now = now_utc or datetime.now(timezone.utc)

    if until:
        end = _parse_isoish_utc(until)
    else:
        end = now

    if since:
        start = _parse_isoish_utc(since)
    else:
        if days is None:
            raise ValueError("Either since or days must be provided.")
        start = end - timedelta(days=int(days))

    if start >= end:
        raise ValueError(f"Invalid backfill range: start >= end (start={start}, end={end})")

    return start, end


def iter_windows(start: datetime, end: datetime, *, chunk_hours: int) -> Iterable[Window]:
    """
    Yield contiguous [start, end) windows of size chunk_hours in UTC.
    """
    if chunk_hours <= 0:
        raise ValueError("chunk_hours must be > 0")

    cur = start
    step = timedelta(hours=int(chunk_hours))
    while cur < end:
        nxt = min(cur + step, end)
        yield Window(start=cur, end=nxt)
        cur = nxt
