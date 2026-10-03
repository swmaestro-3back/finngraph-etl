"""종목 봉 등락률 계산.

등락률(change_rate)은 직전 봉 종가 대비 %다. 일봉은 직전 거래일, 주봉·월봉은 직전 주·월 봉이
기준이고 직전 봉이 없는 첫 봉은 NULL 이다.

봉 적재와 분리된 단계로, 봉을 쓰는 DAG 가 적재 뒤에 같은 구간으로 부른다. 수집이 최근 구간을
수정주가로 다시 받아 과거 종가가 바뀌므로, 당일 행만이 아니라 구간 전체를 다시 계산한다.
구간 첫 봉의 직전 봉은 DB 에서 찾고, since 이후는 끝까지 계산한다 — 가운데 구간만 백필해도
그 뒤 봉의 등락률이 어긋나지 않는다.
"""

from __future__ import annotations

from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.stocks.repositories.postgres.stock_candles import (
    refresh_daily_change_rates,
    refresh_period_change_rates,
)
from pipelines.stocks.repositories.postgres.stocks import fetch_stocks_by_tickers

logger = get_logger(__name__)


def _stock_ids(tickers: list[str] | None) -> list[int] | None:
    if not tickers:
        return None
    with session_scope() as session:
        found, _ = fetch_stocks_by_tickers(session, tickers)
    return [stock_id for stock_id, _ in found]


def run_daily(since: date | None = None, tickers: list[str] | None = None) -> int:
    """since 이후 일봉의 등락률을 다시 계산한다.

    Args:
        since (date | None): 재계산 시작일(포함). 생략하면 오늘 − STOCK_DAILY_LOOKBACK_DAYS
            (일별 수집이 다시 받는 구간).
        tickers (list[str] | None): 단축코드로 대상을 좁힌다. 생략하면 전 종목.

    Returns:
        int: 값이 바뀐 행 수.
    """

    since = since or now_kst().date() - timedelta(days=get_settings().stock_daily_lookback_days)
    stock_ids = _stock_ids(tickers)
    if tickers and not stock_ids:
        return 0
    with session_scope() as session:
        rows = refresh_daily_change_rates(session, since, stock_ids)
    logger.info("일봉 등락률 계산 완료: %d행 변경 (기준 %s)", rows, since)
    return rows


def run_period(since: date | None = None, tickers: list[str] | None = None) -> int:
    """since 가 속한 주·월부터 주봉·월봉의 등락률을 다시 계산한다.

    Args:
        since (date | None): 재계산 기준일. 생략하면 오늘 − STOCK_PERIOD_LOOKBACK_DAYS
            (기간봉 수집이 다시 받는 구간).
        tickers (list[str] | None): 단축코드로 대상을 좁힌다. 생략하면 전 종목.

    Returns:
        int: 값이 바뀐 행 수 (W·M 합계).
    """

    since = since or now_kst().date() - timedelta(days=get_settings().stock_period_lookback_days)
    stock_ids = _stock_ids(tickers)
    if tickers and not stock_ids:
        return 0
    with session_scope() as session:
        rows = refresh_period_change_rates(session, since, stock_ids)
    logger.info("주·월봉 등락률 계산 완료: %d행 변경 (기준 %s)", rows, since)
    return rows


def backfill_since(start: date | None) -> date:
    """백필 DAG 의 시작일 기본값. 봉 백필 job 과 같은 규칙(STOCK_DAILY_BACKFILL_YEARS 전)이다."""

    years = get_settings().stock_daily_backfill_years
    return start or now_kst().date() - relativedelta(years=years)
