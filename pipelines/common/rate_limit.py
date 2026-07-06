from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FixedWindowRateLimiter:
    calls_per_second: int

    def wait(self) -> None:
        raise NotImplementedError("Rate limiter is not implemented yet.")
