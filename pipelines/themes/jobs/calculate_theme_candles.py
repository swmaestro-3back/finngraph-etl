"""테마 지수 일봉·기간봉 계산.

세 지점에서 불린다.

- 장중 DAG(stocks_intraday_candles)·마감 후 DAG(stocks_daily_pipeline): 모두 run_daily() 기본
  lookback(10일) → run_period(since=today−lookback)
- 백필 DAG(themes_backfill_candles): 인자로 받은 구간

구간의 첫 거래일은 t-1 로만 쓰고 둘째 거래일부터 계산한다. 종목 일봉 수집이 최근 10일을
수정주가로 다시 받기 때문에, 구간 밖의 (미조정) 종가와 안의 (조정) 종가를 짝지으면 분할 뒤
지수가 튄다. 그래서 테마 lookback 은 종목 lookback 이하여야 한다.

체인 지수라 앵커(구간 직전 테마 종가)에서 이어 붙인다. 같은 구간을 다시 돌려도 앵커가 같으면
값이 같다. 테마 하나가 실패해도 나머지는 계속 계산하고, 끝에서 실패가 있으면 태스크를
실패시킨다 — 하류(핫테마 발행)가 빠진 테마로 돌지 않게 하려는 것이다.
"""

from __future__ import annotations

from datetime import date, timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.themes.loaders.candles import (
    fetch_anchors,
    fetch_constituent_candles,
    fetch_theme_ids,
    fetch_trading_calendar,
    rebuild_theme_period_candles,
    upsert_theme_daily_candles,
)
from pipelines.themes.transformers.theme_index import compute_theme_candles

logger = get_logger(__name__)


def run_daily(
    start: date | None = None,
    end: date | None = None,
    theme_ids: list[int] | None = None,
) -> int:
    """구간 [start, end] 의 테마 지수 일봉을 계산해 적재한다.

    Args:
        start (date | None): 시작일(포함). 생략하면 end − THEME_DAILY_LOOKBACK_DAYS. 이 날은
            t-1 로만 쓰인다.
        end (date | None): 종료일(포함). 생략하면 오늘.
        theme_ids (list[int] | None): 대상 테마. 생략하면 편입 종목이 있는 전체.

    Returns:
        int: 적재한 행 수.

    Raises:
        RuntimeError: 하나 이상의 테마에서 계산·적재가 실패했을 때.
    """

    settings = get_settings()
    end = end or now_kst().date()
    start = start or end - timedelta(days=settings.theme_daily_lookback_days)

    with session_scope() as session:
        targets = list(theme_ids) if theme_ids else fetch_theme_ids(session)
        calendar = fetch_trading_calendar(session, start, end)
        if len(calendar) < 2:
            logger.info("테마 일봉: 구간 %s~%s 에 계산할 거래일이 없다", start, end)
            return 0
        first_day = calendar[1]
        anchors = fetch_anchors(session, targets, before=first_day)
    since = calendar[0]

    logger.info("테마 일봉 계산 시작: 대상 %d테마, 구간 %s~%s", len(targets), start, end)

    total = 0
    failed: list[int] = []
    for theme_id in targets:
        try:
            with session_scope() as session:
                rows = fetch_constituent_candles(session, theme_id, since, end)
                candles = compute_theme_candles(
                    theme_id, rows, calendar, start, end, anchors.get(theme_id)
                )
                total += upsert_theme_daily_candles(session, candles)
        except Exception:
            logger.exception("테마 일봉 계산 실패: theme_id=%s", theme_id)
            failed.append(theme_id)

    logger.info("테마 일봉 계산 완료: %d행, 실패 %d테마 %s", total, len(failed), failed[:10])
    if failed:
        raise RuntimeError(f"테마 일봉 계산 실패 {len(failed)}건: {failed[:10]}")
    return total


def run_period(since: date | None = None, theme_ids: list[int] | None = None) -> int:
    """since 가 속한 주·월부터의 테마 주봉·월봉을 일봉으로 다시 집계한다.

    Args:
        since (date | None): 재집계 기준일. 생략하면 오늘(이번 주·이번 달만).
        theme_ids (list[int] | None): 대상 테마. 생략하면 전체.

    Returns:
        int: upsert 된 행 수.
    """

    since = since or now_kst().date()
    with session_scope() as session:
        rows = rebuild_theme_period_candles(session, since, theme_ids)
    logger.info("테마 주·월봉 재집계 완료: %d행 (기준 %s)", rows, since)
    return rows
