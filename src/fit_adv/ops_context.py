from __future__ import annotations  # Enables postponed evaluation of type annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class RunContext:
    service: str
    started_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None

    since: Optional[str] = None
    since_hours: Optional[int] = None
    limit: Optional[int] = None

    # path -> {"records": int, "pages": int}
    endpoints: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # label -> path
    outputs: Dict[str, str] = field(default_factory=dict)

    # any other info you want in Slack later
    extra: Dict[str, Any] = field(default_factory=dict)

    def end(self) -> None:
        self.ended_at = time.time()

    @property
    def duration_s(self) -> float:
        end = self.ended_at or time.time()
        return max(0.0, end - self.started_at)

    def add_endpoint(self, path: str, *, records: int, pages: int = 0) -> None:
        self.endpoints[path] = {"records": int(records), "pages": int(pages)}

    def add_output(self, key: str, value: str) -> None:
        self.outputs[str(key)] = str(value)
