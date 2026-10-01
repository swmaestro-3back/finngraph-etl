"""최신 거래일 조회 (stock_candles_daily · stock_valuations_daily)."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 백엔드와 같은 정의다 — 캔들과 밸류에이션이 둘 다 있는 최신 날짜. 일봉만 먼저 들어온
# 날(정규 적재 직후, 장중 백필)을 최신으로 잡으면 백엔드 핫테마 페이로드와 기준일이 어긋나
# 테마 선정이 실패해 버린다.
SELECT_LATEST_TRADE_DATE_SQL = text(
    """
    SELECT LEAST(
             (SELECT MAX(trade_date) FROM stock_candles_daily     WHERE trade_date <= :as_of),
             (SELECT MAX(trade_date) FROM stock_valuations_daily  WHERE trade_date <= :as_of)
           );
    """
)


def fetch_latest_trade_date(as_of: date) -> date | None:
    with session_scope() as session:
        row = session.execute(SELECT_LATEST_TRADE_DATE_SQL, {"as_of": as_of}).scalar()

    return row
