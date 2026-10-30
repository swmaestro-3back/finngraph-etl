from __future__ import annotations

from datetime import date, timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.stocks.repositories.postgres.stock_candles import refresh_daily_week52_ranges
from pipelines.stocks.repositories.postgres.stocks import fetch_stocks_by_tickers

logger = get_logger(__name__)


def run(since: date | None = None, tickers: list[str] | None = None) -> int:
    since = since or now_kst().date() - timedelta(days=get_settings().stock_daily_lookback_days)
    stock_ids = None
    if tickers:
        with session_scope() as session:
            found, _ = fetch_stocks_by_tickers(session, tickers)
        stock_ids = [stock_id for stock_id, _ in found]
        if not stock_ids:
            return 0
    with session_scope() as session:
        rows = refresh_daily_week52_ranges(session, since, stock_ids)
    logger.info("일봉 52주 범위 계산 완료: %d행 변경 (기준 %s)", rows, since)
    return rows
