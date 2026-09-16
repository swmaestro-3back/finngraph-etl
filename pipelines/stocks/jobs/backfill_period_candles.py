"""과거 주봉·월봉 백필.

일별 갱신(collect_daily_candles.run_period)과 같은 KIS 기간별시세를 쓴다. 다만 그쪽은
설정값(STOCK_PERIOD_LOOKBACK_DAYS, 기본 120일)만 되짚어서 과거가 채워지지 않는다.
여기서 구간을 직접 받아 그만큼 받는다.

일봉 백필과 같은 원칙이다 — **지정한 구간을 그대로 받아 upsert 한다.** 이미 채워졌는지
판정하지 않는다. upsert 가 멱등이고, 기간봉은 100건 페이지에 5년이 주봉 3콜·월봉 1콜로
들어가 다시 받는 비용이 작다.

수동 실행이 기본이다(DAG schedule=None).
"""

from __future__ import annotations

from datetime import date

from dateutil.relativedelta import relativedelta

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import now_kst
from pipelines.stocks.extractors.kis import fetch_period_candles
from pipelines.stocks.loaders.candles import upsert_period_candles
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks, fetch_stocks_by_tickers

logger = get_logger(__name__)

CHUNK_SIZE = 50

# 지원하는 봉 종류. KIS FID_PERIOD_DIV_CODE 값이다.
ALL_PERIODS = ("W", "M")


def run(
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    periods: list[str] | None = None,
) -> dict[str, int]:
    """과거 주봉·월봉을 채운다.

    Args:
        tickers (list[str] | None): 단축코드로 대상을 좁힌다. 생략하면 서비스 대상 전체.
        start (date | None): 시작일(포함). 생략하면 STOCK_DAILY_BACKFILL_YEARS 전.
        end (date | None): 종료일(포함). 생략하면 오늘.
        periods (list[str] | None): 받을 봉 종류. 생략하면 주봉·월봉 둘 다.

    Returns:
        dict[str, int]: 처리 결과 요약. Airflow XCom 으로 노출된다.

    Raises:
        ValueError: periods 에 'W'·'M' 이 아닌 값이 있을 때.
    """

    wanted = tuple(periods) if periods else ALL_PERIODS
    invalid = [p for p in wanted if p not in ALL_PERIODS]
    if invalid:
        raise ValueError(f"periods 는 {list(ALL_PERIODS)} 중에서 고른다: {invalid}")

    settings = get_settings()
    today = now_kst().date()
    # 일봉과 같은 연수를 쓴다. 기간봉만 따로 둘 이유가 없고, 차트가 같은 구간을 그린다.
    window_start = start or today - relativedelta(years=settings.stock_daily_backfill_years)
    window_end = end or today

    with session_scope() as session:
        if tickers:
            targets, missing = fetch_stocks_by_tickers(session, tickers)
            if missing:
                logger.warning("찾지 못한 단축코드 %d개: %s", len(missing), missing)
        else:
            targets = fetch_serviceable_stocks(session)

    logger.info(
        "기간봉 백필 시작: 대상 %d종목, 구간 %s~%s, 봉 %s",
        len(targets),
        window_start,
        window_end,
        list(wanted),
    )

    client = get_kis_client()

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        candles = []
        for _, ticker in chunk:
            for period in wanted:
                try:
                    candles.extend(
                        fetch_period_candles(
                            ticker, period, window_start, window_end, client=client
                        )
                    )
                except Exception:
                    # 한 종목·한 봉의 실패가 배치 전체를 죽이면 안 된다.
                    logger.exception(
                        "기간봉 수집 실패: ticker=%s period=%s 구간=%s~%s",
                        ticker,
                        period,
                        window_start,
                        window_end,
                    )
                    failed.append(f"{ticker}:{period}")

        if not candles:
            continue

        with session_scope() as session:
            total_rows += upsert_period_candles(session, candles)

    logger.info("기간봉 백필 완료: %d행 적재, 실패 %d건 %s", total_rows, len(failed), failed[:10])

    return {"targets": len(targets), "rows": total_rows, "failed": len(failed)}
