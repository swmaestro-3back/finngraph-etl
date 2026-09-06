from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class DailyCandle:
    """일봉.

    extractor는 종목을 단축코드로 다루고 stock_id를 모른다. DB 키인 stock_id로의 변환은
    적재 시점에 loader가 한다 — 원천마다 stocks를 다시 조회하지 않게 하려는 것이다.

    Attributes:
        ticker (str): 단축코드.
        trade_date (date): 거래일.
        volume (int): 거래량(주).
        trade_value (int | None): 거래대금(원). 원천이 주지 않으면 None.
    """

    ticker: str
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_value: int | None = None


@dataclass(frozen=True)
class PeriodCandle:
    """주봉·월봉.

    Attributes:
        period (str): 'W'(주) 또는 'M'(월).
        base_date (date): 해당 기간의 마지막 거래일. KIS가 그렇게 준다.
    """

    ticker: str
    period: str
    base_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_value: int | None = None
