"""과거 일봉 백필.

FinanceDataReader로 종목당 1회 호출해 전 구간을 받는다. 이미 적재된 종목은 마지막
거래일 다음날부터만 받아, 재실행이 처음부터 다시 긁지 않게 한다.

수동 실행이 기본이다(DAG schedule=None). 2,600종목 × 1회라 한 번 돌리면 끝이고,
매일 도는 갱신은 KIS 쪽(collect_daily_candles)이 맡는다.
"""

from __future__ import annotations

from datetime import date, timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import now_kst
from pipelines.stocks.extractors.finance_data_reader import fetch_daily_candles
from pipelines.stocks.loaders.candles import fetch_latest_daily_candle_dates, upsert_daily_candles
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks

logger = get_logger(__name__)

CHUNK_SIZE = 50


def run(limit: int | None = None) -> None:
    """서비스 대상 종목의 과거 일봉을 채운다.

    Args:
        limit (int | None): 처리할 종목 수 상한. 시험 실행용.
    """

    settings = get_settings()
    today = now_kst().date()
    earliest = date(today.year - settings.stock_daily_backfill_years, today.month, today.day)

    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)
        latest_dates = fetch_latest_daily_candle_dates(session)

    logger.info("일봉 백필 시작: 대상 %d종목, 최초 시작일 %s", len(targets), earliest)

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        candles = []
        for stock_id, ticker in chunk:
            start = _start_date(latest_dates.get(stock_id), earliest)
            if start > today:
                continue

            try:
                candles.extend(fetch_daily_candles(ticker, start, today))
            except Exception:
                # 한 종목의 실패가 배치 전체를 죽이면 안 된다. 남은 종목을 계속 처리하고
                # 실패 목록만 모아 마지막에 보고한다.
                logger.exception("일봉 수집 실패: ticker=%s start=%s", ticker, start)
                failed.append(ticker)

        if not candles:
            continue

        with session_scope() as session:
            total_rows += upsert_daily_candles(session, candles, source="FDR")

    logger.info("일봉 백필 완료: %d행 적재, 실패 %d종목 %s", total_rows, len(failed), failed[:10])


def _start_date(latest: date | None, earliest: date) -> date:
    """이미 적재된 마지막 거래일 다음날. 없으면 최초 시작일."""

    if latest is None:
        return earliest
    return latest + timedelta(days=1)
