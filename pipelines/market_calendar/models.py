from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class MarketDay:
    trade_date: date
    is_open: bool
    is_business_day: bool
    is_settlement_day: bool
    weekday_code: str


@dataclass(frozen=True)
class CalendarEvent:
    event_date: date
    kind: str
    ticker: str
    stock_name: str
    source: str
    source_key: str
    basis_date: date
    end_date: date | None = None
    amount: Decimal | None = None
    ratio: Decimal | None = None
    label: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IpoOffering:
    ticker: str
    name: str
    subscr_start: date
    subscr_end: date
    offer_price: Decimal | None
    pay_date: date | None
    refund_date: date | None
    listing_date: date | None
    lead_managers: str | None
    basis_date: date
    detail: dict[str, Any] = field(default_factory=dict)
