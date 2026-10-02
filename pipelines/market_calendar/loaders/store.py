from __future__ import annotations

import json
from collections.abc import Collection
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import CalendarEvent, IpoOffering, MarketDay

UPSERT_MARKET_DAY_SQL = text(
    """
    INSERT INTO market_days (
      trade_date, is_open, is_business_day, is_settlement_day, weekday_code, updated_at
    )
    VALUES (
      :trade_date, :is_open, :is_business_day, :is_settlement_day, :weekday_code, now()
    )
    ON CONFLICT (trade_date) DO UPDATE SET
      is_open = EXCLUDED.is_open,
      is_business_day = EXCLUDED.is_business_day,
      is_settlement_day = EXCLUDED.is_settlement_day,
      weekday_code = EXCLUDED.weekday_code,
      updated_at = now()
    """
)

SELECT_MARKET_DAYS_SQL = text(
    """
    SELECT trade_date, is_open, is_business_day, is_settlement_day, weekday_code
      FROM market_days
     WHERE trade_date BETWEEN :start AND :end
     ORDER BY trade_date
    """
)

COUNT_EVENTS_SQL = text(
    """
    SELECT COUNT(*)
      FROM stock_calendar_events
     WHERE source = :source AND basis_date BETWEEN :start AND :end
    """
)

DELETE_EVENTS_SQL = text(
    """
    DELETE FROM stock_calendar_events
     WHERE source = :source AND basis_date BETWEEN :start AND :end
       AND NOT (basis_date = ANY(CAST(:keep AS date[])))
    """
)

DELETE_EVENTS_BY_KEY_SQL = text(
    """
    DELETE FROM stock_calendar_events
     WHERE source = :source AND source_key = ANY(:keys)
    """
)

INSERT_EVENT_SQL = text(
    """
    INSERT INTO stock_calendar_events (
      event_date, kind, ticker, stock_name, source, source_key, basis_date,
      end_date, amount, ratio, label, detail, updated_at
    )
    VALUES (
      :event_date, :kind, :ticker, :stock_name, :source, :source_key, :basis_date,
      :end_date, :amount, :ratio, :label, CAST(:detail AS jsonb), now()
    )
    """
)

COUNT_IPOS_SQL = text("SELECT COUNT(*) FROM ipo_offerings WHERE basis_date BETWEEN :start AND :end")

DELETE_IPOS_SQL = text(
    """
    DELETE FROM ipo_offerings
     WHERE basis_date BETWEEN :start AND :end
       AND NOT (basis_date = ANY(CAST(:keep AS date[])))
    """
)

INSERT_IPO_SQL = text(
    """
    INSERT INTO ipo_offerings (
      ticker, name, subscr_start, subscr_end, offer_price, pay_date, refund_date,
      listing_date, lead_managers, basis_date, detail, updated_at
    )
    VALUES (
      :ticker, :name, :subscr_start, :subscr_end, :offer_price, :pay_date, :refund_date,
      :listing_date, :lead_managers, :basis_date, CAST(:detail AS jsonb), now()
    )
    ON CONFLICT (ticker, subscr_start) DO UPDATE SET
      name = EXCLUDED.name,
      subscr_end = EXCLUDED.subscr_end,
      offer_price = EXCLUDED.offer_price,
      pay_date = EXCLUDED.pay_date,
      refund_date = EXCLUDED.refund_date,
      listing_date = EXCLUDED.listing_date,
      lead_managers = EXCLUDED.lead_managers,
      basis_date = EXCLUDED.basis_date,
      detail = EXCLUDED.detail,
      updated_at = now()
    """
)

SELECT_ACTIVE_TICKERS_SQL = text(
    """
    SELECT ticker
      FROM stocks
     WHERE is_active AND market IN ('KOSPI', 'KOSDAQ')
     ORDER BY ticker
    """
)


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def upsert_market_days(session: Session, days: list[MarketDay]) -> int:
    unique = {day.trade_date: day for day in days}
    if not unique:
        return 0
    session.execute(
        UPSERT_MARKET_DAY_SQL,
        [
            {
                "trade_date": day.trade_date,
                "is_open": day.is_open,
                "is_business_day": day.is_business_day,
                "is_settlement_day": day.is_settlement_day,
                "weekday_code": day.weekday_code,
            }
            for day in unique.values()
        ],
    )
    return len(unique)


def select_market_days(session: Session, start: date, end: date) -> list[MarketDay]:
    rows = session.execute(SELECT_MARKET_DAYS_SQL, {"start": start, "end": end})
    return [
        MarketDay(
            trade_date=row.trade_date,
            is_open=row.is_open,
            is_business_day=row.is_business_day,
            is_settlement_day=row.is_settlement_day,
            weekday_code=row.weekday_code,
        )
        for row in rows
    ]


def count_events(session: Session, source: str, start: date, end: date) -> int:
    params = {"source": source, "start": start, "end": end}
    return int(session.execute(COUNT_EVENTS_SQL, params).scalar_one())


def replace_events(
    session: Session,
    source: str,
    start: date,
    end: date,
    events: list[CalendarEvent],
    keep_basis_dates: Collection[date] = (),
) -> int:
    keep = sorted(set(keep_basis_dates))
    unique = {
        (event.source_key, event.kind): event
        for event in events
        if event.source == source
        and start <= event.basis_date <= end
        and event.basis_date not in keep
    }
    session.execute(DELETE_EVENTS_SQL, {"source": source, "start": start, "end": end, "keep": keep})
    if unique:
        keys = sorted({event.source_key for event in unique.values()})
        session.execute(DELETE_EVENTS_BY_KEY_SQL, {"source": source, "keys": keys})
        session.execute(
            INSERT_EVENT_SQL,
            [
                {
                    "event_date": event.event_date,
                    "kind": event.kind,
                    "ticker": event.ticker,
                    "stock_name": event.stock_name,
                    "source": event.source,
                    "source_key": event.source_key,
                    "basis_date": event.basis_date,
                    "end_date": event.end_date,
                    "amount": event.amount,
                    "ratio": event.ratio,
                    "label": event.label,
                    "detail": _json(event.detail),
                }
                for event in unique.values()
            ],
        )
    return len(unique)


def count_ipo_offerings(session: Session, start: date, end: date) -> int:
    return int(session.execute(COUNT_IPOS_SQL, {"start": start, "end": end}).scalar_one())


def replace_ipo_offerings(
    session: Session,
    start: date,
    end: date,
    offerings: list[IpoOffering],
    keep_basis_dates: Collection[date] = (),
) -> int:
    keep = sorted(set(keep_basis_dates))
    unique = {
        (offering.ticker, offering.subscr_start): offering
        for offering in offerings
        if start <= offering.basis_date <= end and offering.basis_date not in keep
    }
    session.execute(DELETE_IPOS_SQL, {"start": start, "end": end, "keep": keep})
    if unique:
        session.execute(
            INSERT_IPO_SQL,
            [
                {
                    "ticker": offering.ticker,
                    "name": offering.name,
                    "subscr_start": offering.subscr_start,
                    "subscr_end": offering.subscr_end,
                    "offer_price": offering.offer_price,
                    "pay_date": offering.pay_date,
                    "refund_date": offering.refund_date,
                    "listing_date": offering.listing_date,
                    "lead_managers": offering.lead_managers,
                    "basis_date": offering.basis_date,
                    "detail": _json(offering.detail),
                }
                for offering in unique.values()
            ],
        )
    return len(unique)


def select_active_tickers(session: Session) -> list[str]:
    return [row.ticker for row in session.execute(SELECT_ACTIVE_TICKERS_SQL)]
