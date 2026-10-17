from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from pipelines.stocks.jobs.collect_daily_candles import drop_preopen_placeholders
from pipelines.stocks.types import DailyCandle

KST = ZoneInfo("Asia/Seoul")
TODAY = date(2030, 3, 11)


def candle(trade_date: date, volume: int) -> DailyCandle:
    price = Decimal("1000")
    return DailyCandle(
        ticker="900001",
        trade_date=trade_date,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=volume,
        trade_value=None,
    )


def test_before_regular_open_drops_todays_zero_volume_row() -> None:
    rows = [candle(date(2030, 3, 10), 0), candle(TODAY, 0)]

    kept = drop_preopen_placeholders(rows, datetime(2030, 3, 11, 8, 5, tzinfo=KST))

    assert kept == [rows[0]]


def test_before_regular_open_keeps_todays_traded_row() -> None:
    rows = [candle(TODAY, 1200)]

    assert drop_preopen_placeholders(rows, datetime(2030, 3, 11, 8, 59, tzinfo=KST)) == rows


def test_from_regular_open_keeps_todays_zero_volume_row() -> None:
    rows = [candle(TODAY, 0)]

    assert drop_preopen_placeholders(rows, datetime(2030, 3, 11, 9, 0, tzinfo=KST)) == rows
