from __future__ import annotations

from pipelines.common.config import get_settings
from pipelines.common.rate_limit import FixedWindowRateLimiter
from pipelines.common.types import MinuteCandle


class KisClient:
    def __init__(self) -> None:
        settings = get_settings()
        self.app_key = settings.kis_app_key
        self.app_secret = settings.kis_app_secret
        self.rate_limiter = FixedWindowRateLimiter(settings.kis_rate_limit_per_second)

    def fetch_intraday_1m(self, symbol: str) -> list[MinuteCandle]:
        self.rate_limiter.wait()
        raise NotImplementedError("KIS 1m candle extraction is not implemented yet.")
