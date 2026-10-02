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


@dataclass(frozen=True)
class IpoFilingHead:
    corp_code: str
    corp_name: str
    corp_cls: str
    status: str
    spac: bool
    first_rcept_no: str
    first_filed_on: date
    latest_rcept_no: str
    latest_report_nm: str
    document_rcept_no: str | None = None
    terms_rcept_no: str | None = None


@dataclass(frozen=True)
class OfferingTerms:
    subscr_start: date | None = None
    subscr_end: date | None = None
    pay_date: date | None = None
    offer_price: Decimal | None = None
    offer_shares: int | None = None
    offer_amount: Decimal | None = None
    offer_method: str | None = None
    underwriters: list[dict[str, Any]] = field(default_factory=list)
    fund_uses: list[dict[str, Any]] = field(default_factory=list)
    sellers: list[dict[str, Any]] = field(default_factory=list)
    putback: dict[str, Any] | None = None


@dataclass(frozen=True)
class StoredFiling:
    corp_code: str
    first_rcept_no: str
    first_filed_on: date
    latest_rcept_no: str
    has_terms: bool
    description_ready: bool


@dataclass(frozen=True)
class CompanyRef:
    corp_code: str
    company_id: int
    has_profile: bool
    has_description: bool


@dataclass(frozen=True)
class LinkTarget:
    corp_code: str
    corp_name: str
    subscr_start: date | None
    ticker: str | None


@dataclass(frozen=True)
class KsdOfferingRef:
    ticker: str
    name: str
    subscr_start: date
