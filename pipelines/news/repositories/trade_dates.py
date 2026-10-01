"""거래일 범위 조회 (stock_candles_daily · stock_valuations_daily)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# settled 는 백엔드와 같은 정의다 — 캔들과 밸류에이션이 둘 다 있는 최신 날짜. latest 는 일봉만
# 있는 최신 날짜다. 장중에는 일봉만 먼저 들어오고(stocks_intraday_candles) 백엔드가 그 날짜로
# 핫테마를 재발행하므로, 핫테마 기준일은 settled~latest 어디든 될 수 있다.
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
