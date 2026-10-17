"""market_days 테이블 조회·적재."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import MarketDay

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
