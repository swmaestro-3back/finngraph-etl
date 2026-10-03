"""일봉으로 이번 주·이번 달 봉을 갱신한다. 장 마감 후 KIS 공식 주·월봉이 같은 키를 덮어쓴다."""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst

logger = get_logger(__name__)

LOOKBACK_DAYS = 7


def run() -> int:
    from pipelines.stocks.repositories.postgres.stock_candles import (
        aggregate_current_period_candles,
    )

    since = now_kst().date() - timedelta(days=LOOKBACK_DAYS)
    with session_scope() as session:
        rows = aggregate_current_period_candles(session, since)
    logger.info("주·월봉 합성 완료: %d행 (기준 %s 이후 일봉 보유 종목)", rows, since)
    return rows
