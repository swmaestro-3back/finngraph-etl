from __future__ import annotations

from sqlalchemy.orm import Session

from pipelines.common.types import DailyCandle, MinuteCandle


def upsert_daily_candles(session: Session, candles: list[DailyCandle]) -> int:
    raise NotImplementedError("Daily candle upsert is not implemented yet.")


def upsert_minute_candles(session: Session, interval: str, candles: list[MinuteCandle]) -> int:
    raise NotImplementedError(f"{interval} candle upsert is not implemented yet.")

