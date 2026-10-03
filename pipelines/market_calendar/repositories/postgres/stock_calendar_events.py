"""stock_calendar_events 테이블 조회·적재."""

from __future__ import annotations

import json
from collections.abc import Collection
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import CalendarEvent

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


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


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
