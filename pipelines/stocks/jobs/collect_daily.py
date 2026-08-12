"""일봉·주봉·월봉 일별 갱신.

과거 구간은 FDR 백필(backfill_daily)이 채우고, 여기서는 KIS 기간별시세로 최근 구간만
덧붙인다. 같은 (stock_id, trade_date)를 양쪽이 채울 수 있으므로 upsert가 덮어쓴다.

lookback을 하루가 아니라 열흘로 잡는 이유는 정정·수정주가 반영 때문이다. 액면분할이
있으면 과거 며칠의 가격이 바뀌는데, 하루만 받으면 그 변경을 영영 못 받는다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.batching import chunked
from pipelines.common.config import get_settings
from pipelines.common.database import session_scope
from pipelines.common.kis import get_kis_client
from pipelines.common.logging import get_logger
from pipelines.common.time import now_kst
from pipelines.stocks.extractors.kis import fetch_daily_candles, fetch_period_candles
from pipelines.stocks.loaders.candles import upsert_daily_candles, upsert_period_candles
from pipelines.stocks.loaders.symbols import fetch_serviceable_stocks

logger = get_logger(__name__)

CHUNK_SIZE = 100

PERIODS = ("W", "M")


def run(limit: int | None = None) -> None:
    """최근 구간의 일봉을 KIS에서 받아 갱신한다."""

    settings = get_settings()
    today = now_kst().date()
    start = today - timedelta(days=settings.stock_daily_lookback_days)

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)

    logger.info("일봉 갱신 시작: 대상 %d종목, 구간 %s~%s", len(targets), start, today)

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        candles = []
        for _, symbol in chunk:
            try:
                candles.extend(fetch_daily_candles(symbol, start, today, client=client))
            except Exception:
                logger.exception("일봉 갱신 실패: symbol=%s", symbol)
                failed.append(symbol)

        if candles:
            with session_scope() as session:
                total_rows += upsert_daily_candles(session, candles, source="KIS")

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
        for _, symbol in chunk:
            for period in PERIODS:
                try:
                    candles.extend(
                        fetch_period_candles(symbol, period, start, today, client=client)
                    )
                except Exception:
                    logger.exception("기간봉 수집 실패: symbol=%s period=%s", symbol, period)
                    failed.append(f"{symbol}:{period}")

        if candles:
            with session_scope() as session:
                total_rows += upsert_period_candles(session, candles)

    logger.info("기간봉 갱신 완료: %d행, 실패 %d건 %s", total_rows, len(failed), failed[:10])
