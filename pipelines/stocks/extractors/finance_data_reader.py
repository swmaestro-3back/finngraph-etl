from __future__ import annotations

from datetime import date

from pipelines.common.types import DailyCandle
from pipelines.stocks.models import StockSymbol


def fetch_symbols() -> list[StockSymbol]:
    raise NotImplementedError("FinanceDataReader symbol extraction is not implemented yet.")


def fetch_daily_candles(symbol: str, start: date, end: date) -> list[DailyCandle]:
    raise NotImplementedError("FinanceDataReader daily candle extraction is not implemented yet.")

