"""일봉·주봉·월봉 일별 갱신.

과거 구간은 FDR 백필(backfill_daily_candles)이 채우고, 여기서는 KIS 기간별시세로 최근 구간만
덧붙인다. 같은 (stock_id, trade_date)를 양쪽이 채울 수 있으므로 upsert가 덮어쓴다.

lookback을 하루가 아니라 열흘로 잡는 이유는 정정·수정주가 반영 때문이다. 액면분할이
있으면 과거 며칠의 가격이 바뀌는데, 하루만 받으면 그 변경을 영영 못 받는다.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import now_kst
from pipelines.stocks.extractors.kis import fetch_daily_candles, fetch_period_candles
from pipelines.stocks.repositories.postgres.stock_candles import (
    upsert_daily_candles,
    upsert_period_candles,
)
from pipelines.stocks.repositories.postgres.stocks import fetch_serviceable_stocks
from pipelines.stocks.types import DailyCandle

logger = get_logger(__name__)

CHUNK_SIZE = 100

UPSERT_BATCH_SIZE = 1000

PERIODS = ("W", "M")

REGULAR_OPEN = time(9, 0)


def drop_preopen_placeholders(candles: list[DailyCandle], now: datetime) -> list[DailyCandle]:
    if now.time() >= REGULAR_OPEN:
        return candles
    today = now.date()
    return [c for c in candles if not (c.trade_date == today and c.volume == 0)]


def run(limit: int | None = None) -> None:
    """최근 구간의 일봉을 KIS에서 받아 갱신한다."""

    settings = get_settings()
    now = now_kst()
    today = now.date()
    start = today - timedelta(days=settings.stock_daily_lookback_days)

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)

    logger.info("일봉 갱신 시작: 대상 %d종목, 구간 %s~%s", len(targets), start, today)

    total_rows = 0
    failed: list[str] = []
    candles: list[DailyCandle] = []

    for _, ticker in targets:
        try:
            fetched = fetch_daily_candles(ticker, start, today, client=client)
            candles.extend(drop_preopen_placeholders(fetched, now))
        except Exception:
            logger.exception("일봉 갱신 실패: ticker=%s", ticker)
            failed.append(ticker)

    if candles:
        with session_scope() as session:
            for batch in chunked(candles, UPSERT_BATCH_SIZE):
                total_rows += upsert_daily_candles(session, batch, source="KIS")

    logger.info("일봉 갱신 완료: %d행, 실패 %d종목 %s", total_rows, len(failed), failed[:10])


def run_period(limit: int | None = None, lookback_days: int | None = None) -> None:
    """주봉·월봉을 갱신한다.

    Args:
        limit (int | None): 처리할 종목 수 상한.
        lookback_days (int | None): 조회 구간 길이. 생략하면 설정값(기본 120일)을 쓴다.
            최초 적재로 전체 이력이 필요하면 3650 같은 큰 값을 넘긴다.
    """

    settings = get_settings()
    today = now_kst().date()
    start = today - timedelta(days=lookback_days or settings.stock_period_lookback_days)

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)

    logger.info("기간봉 갱신 시작: 대상 %d종목, 구간 %s~%s", len(targets), start, today)

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        candles = []
        for _, ticker in chunk:
            for period in PERIODS:
                try:
                    candles.extend(
                        fetch_period_candles(ticker, period, start, today, client=client)
                    )
                except Exception:
                    logger.exception("기간봉 수집 실패: ticker=%s period=%s", ticker, period)
                    failed.append(f"{ticker}:{period}")

        if candles:
            with session_scope() as session:
                total_rows += upsert_period_candles(session, candles)

    logger.info("기간봉 갱신 완료: %d행, 실패 %d건 %s", total_rows, len(failed), failed[:10])
