"""과거 일봉 백필.

일별 갱신(collect_daily_candles)과 같은 KIS 기간별시세를 쓴다. 원천이 하나여야 거래대금이
채워지고 액면분할 종목의 거래량도 어긋나지 않는다 — FDR 은 거래대금을 아예 주지 않았고
분할 반영 기준이 KIS 와 달랐다. 대신 100건씩 끊겨 종목당 여러 번 호출한다.

**지정한 구간을 그대로 받아 upsert 한다. "이미 채워졌으니 건너뛴다"는 판정을 두지 않는다.**
적재 여부를 정확히 알려면 구간 안에 구멍이 없는지 봐야 하는데, 가장 오래된 날짜만으로는
가운데가 빈 경우(a-공백-c)를 못 본다. 부분적으로만 맞는 판정은 "채워졌다고 착각하고
영영 안 받는" 결과를 낳아 없느니만 못하다. upsert 가 멱등이고 전 종목 5년이 1시간 20분이라,
다시 받는 편이 싸고 확실하다. 좁혀 돌리고 싶으면 종목·기간을 지정한다.

수동 실행이 기본이다(DAG schedule=None). 매일 도는 갱신은 collect_daily_candles 가 맡는다.
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
from pipelines.stocks.extractors.kis import fetch_daily_candles
from pipelines.stocks.loaders.candles import upsert_daily_candles
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks, fetch_stocks_by_tickers

logger = get_logger(__name__)

CHUNK_SIZE = 50


def run(
    tickers: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, int]:
    """과거 일봉을 채운다.

    Args:
        tickers (list[str] | None): 단축코드로 대상을 좁힌다. 생략하면 서비스 대상 전체.
        start (date | None): 시작일(포함). 생략하면 STOCK_DAILY_BACKFILL_YEARS 전.
        end (date | None): 종료일(포함). 생략하면 오늘.

    Returns:
        dict[str, int]: 처리 결과 요약. Airflow XCom 으로 노출된다.
    """

    settings = get_settings()
    today = now_kst().date()
    window_start = start or today - relativedelta(years=settings.stock_daily_backfill_years)
    window_end = end or today

    with session_scope() as session:
        if tickers:
            targets, missing = fetch_stocks_by_tickers(session, tickers)
            if missing:
                logger.warning("찾지 못한 단축코드 %d개: %s", len(missing), missing)
        else:
            targets = fetch_serviceable_stocks(session)

    logger.info("일봉 백필 시작: 대상 %d종목, 구간 %s~%s", len(targets), window_start, window_end)

    client = get_kis_client()

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        candles = []
        for _, ticker in chunk:
            try:
                candles.extend(fetch_daily_candles(ticker, window_start, window_end, client=client))
            except Exception:
                # 한 종목의 실패가 배치 전체를 죽이면 안 된다. 남은 종목을 계속 처리하고
                # 실패 목록만 모아 마지막에 보고한다.
                logger.exception(
                    "일봉 수집 실패: ticker=%s 구간=%s~%s", ticker, window_start, window_end
                )
                failed.append(ticker)

        if not candles:
            continue

        with session_scope() as session:
            total_rows += upsert_daily_candles(session, candles)

    logger.info("일봉 백필 완료: %d행 적재, 실패 %d종목 %s", total_rows, len(failed), failed[:10])

    return {"targets": len(targets), "rows": total_rows, "failed": len(failed)}
