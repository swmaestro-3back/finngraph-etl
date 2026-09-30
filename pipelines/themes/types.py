from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class ConstituentCandle:
    """테마 지수 계산에 들어가는 구성 종목의 하루치 일봉.

    theme_stocks ⋈ stocks ⋈ stock_candles_daily 한 행이다. listed_shares 는 시총 계산의
    분모가 아니라 곱셈 인자(직전 종가 × 상장주식수)로 쓰이며, NULL·0 이면 그 종목은 그날
    계산에서 빠진다.

    Attributes:
        trade_value (int | None): 거래대금(원). 원천이 주지 않으면 None.
        listed_shares (int | None): 상장주식수(주). stocks 의 현재 값.
    """

    stock_id: int
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_value: int | None
    listed_shares: int | None


@dataclass(frozen=True)
class ThemeCandle:
    """테마 지수 일봉. OHLC 는 지수값(기준 1000), 거래량·거래대금은 참여 종목 합이다."""

    theme_id: int
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_value: int | None = None
