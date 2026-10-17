from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.jobs import (
    backfill_daily_candles,
    backfill_period_candles,
    calculate_change_rates,
)
from pipelines.stocks.repositories.postgres.stocks import fetch_active_stock_ids
from pipelines.themes.jobs import calculate_theme_candles
from pipelines.themes.repositories.postgres.theme_stocks import fetch_theme_ids_for_stocks

logger = get_logger(__name__)

MAX_REPAIR_TICKERS = 20


def run(tickers: list[str]) -> int:
    if not tickers:
        return 0

    if len(tickers) > MAX_REPAIR_TICKERS:
        logger.error(
            "수정주가 복구 대상 과다: %d종목, 상위 %d개 %s",
            len(tickers),
            MAX_REPAIR_TICKERS,
            tickers[:MAX_REPAIR_TICKERS],
        )
        return 0

    since = calculate_change_rates.backfill_since(None)
    repaired: list[str] = []

    for ticker in tickers:
        try:
            backfill_daily_candles.run(tickers=[ticker])
            backfill_period_candles.run(tickers=[ticker])
            calculate_change_rates.run_daily(since=since, tickers=[ticker])
            calculate_change_rates.run_period(since=since, tickers=[ticker])
        except Exception:
            logger.exception("수정주가 복구 실패: ticker=%s", ticker)
            continue
        repaired.append(ticker)

    if repaired:
        theme_ids: list[int] = []
        try:
            with session_scope() as session:
                stock_ids = fetch_active_stock_ids(session)
                repaired_ids = [stock_ids[ticker] for ticker in repaired if ticker in stock_ids]
                theme_ids = fetch_theme_ids_for_stocks(session, repaired_ids)

            if theme_ids:
                calculate_theme_candles.run_daily(start=since, theme_ids=theme_ids)
                calculate_theme_candles.run_period(since=since, theme_ids=theme_ids)
                calculate_theme_candles.run_change_rates(since=since, theme_ids=theme_ids)
        except Exception:
            logger.exception("수정주가 복구 뒤 테마 지수 재계산 실패: theme_ids=%s", theme_ids)

    logger.info("수정주가 복구 완료: %d/%d종목 %s", len(repaired), len(tickers), repaired)

    return len(repaired)
