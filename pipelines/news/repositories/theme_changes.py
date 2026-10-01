"""테마 등락률 조회 (themes · theme_stocks · stock_candles_daily).

기준일은 as_of 이하의 최신 거래일이다. 종목 등락률은 그 날 종가 / 직전 거래일 종가 − 1 이고,
테마 등락률은 편입 종목 등락률의 단순 평균이다. 봉이 있는 편입 종목이 MIN_STOCKS 미만인
테마는 표본이 작아 제외한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

MIN_STOCKS = 3

SELECT_TRADE_DATES_SQL = text(
    """
    SELECT LEAST(c.trade_date, v.trade_date) AS settled,
           c.trade_date                     AS latest
      FROM (SELECT MAX(trade_date) AS trade_date
              FROM stock_candles_daily
             WHERE trade_date <= :as_of) c,
           (SELECT MAX(trade_date) AS trade_date
              FROM stock_valuations_daily
             WHERE trade_date <= :as_of) v;
    """
)

SELECT_THEME_CHANGES_SQL = text(
    """
    WITH latest AS (
        SELECT LEAST(
                 (SELECT MAX(trade_date) FROM stock_candles_daily    WHERE trade_date <= :as_of),
                 (SELECT MAX(trade_date) FROM stock_valuations_daily WHERE trade_date <= :as_of)
               ) AS trade_date
    ),
    stock_changes AS (
        SELECT c.stock_id,
               (c.close / NULLIF(p.close, 0) - 1) * 100 AS change
          FROM stock_candles_daily c
          JOIN latest ON c.trade_date = latest.trade_date
          JOIN LATERAL (
               SELECT close
                 FROM stock_candles_daily p
                WHERE p.stock_id = c.stock_id
                  AND p.trade_date < c.trade_date
                ORDER BY p.trade_date DESC
                LIMIT 1
          ) p ON true
    )
    SELECT t.id,
           t.name,
           latest.trade_date,
           AVG(sc.change) AS change,
           COUNT(*)       AS stock_count
      FROM themes t
      JOIN theme_stocks ts ON ts.theme_id = t.id
      JOIN stocks s        ON s.id = ts.stock_id AND s.is_active
      JOIN stock_changes sc ON sc.stock_id = s.id
      CROSS JOIN latest
     WHERE sc.change IS NOT NULL
     GROUP BY t.id, t.name, latest.trade_date
    HAVING COUNT(*) >= :min_stocks
     ORDER BY t.id;
    """
)


@dataclass(frozen=True)
class ThemeChange:
    theme_id: int
    name: str
    trade_date: date
    change: float  # 편입 종목 등락률(%) 단순 평균
    stock_count: int


@dataclass(frozen=True)
class TradeDates:
    settled: date | None
    latest: date | None

    def covers(self, trade_date: date | None) -> bool:
        if trade_date is None or self.settled is None or self.latest is None:
            return False
        return self.settled <= trade_date <= self.latest


def fetch_trade_dates(as_of: date) -> TradeDates:
    with session_scope() as session:
        settled, latest = session.execute(SELECT_TRADE_DATES_SQL, {"as_of": as_of}).one()

    return TradeDates(settled=settled, latest=latest)


def fetch_theme_changes(as_of: date, min_stocks: int = MIN_STOCKS) -> list[ThemeChange]:
    """as_of 이하 최신 거래일의 테마 등락률"""

    with session_scope() as session:
        rows = session.execute(
            SELECT_THEME_CHANGES_SQL, {"as_of": as_of, "min_stocks": min_stocks}
        ).fetchall()

    return [
        ThemeChange(
            theme_id=int(theme_id),
            name=name,
            trade_date=trade_date,
            change=float(change),
            stock_count=int(stock_count),
        )
        for theme_id, name, trade_date, change, stock_count in rows
    ]
