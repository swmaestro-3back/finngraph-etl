from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class FixedWindowRateLimiter:
    calls_per_second: int
    _window_started_at: float = 0
    _calls_in_window: int = 0

    def wait(self) -> None:
        if self.calls_per_second <= 0:
            return

        now = time.monotonic()
        if self._window_started_at == 0 or now - self._window_started_at >= 1:
            self._window_started_at = now
            self._calls_in_window = 0

        if self._calls_in_window >= self.calls_per_second:
            time.sleep(max(0, 1 - (now - self._window_started_at)))
            self._window_started_at = time.monotonic()
            self._calls_in_window = 0

        self._calls_in_window += 1
