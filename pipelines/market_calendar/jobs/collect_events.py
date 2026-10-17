from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.market_calendar.extractors.kis_ksd import (
    BONUS_ISSUE,
    DIVIDEND,
    RIGHTS_ISSUE,
    SHAREHOLDER_MEETING,
    FetchStats,
    KsdEndpoint,
    fetch_ksd_rows,
)
from pipelines.market_calendar.models import CalendarEvent, MarketDay
from pipelines.market_calendar.repositories.postgres.market_days import select_market_days
from pipelines.market_calendar.repositories.postgres.stock_calendar_events import (
    count_events,
    replace_events,
)
from pipelines.market_calendar.transformers.events import (
    SOURCE_AGM,
    SOURCE_BONUS,
    SOURCE_DIVIDEND,
    SOURCE_RIGHTS,
    to_agm_events,
    to_bonus_events,
    to_dividend_events,
    to_rights_events,
)
from pipelines.stocks.repositories.postgres.stocks import select_active_tickers

logger = get_logger(__name__)

LOOKBACK_DAYS = 90
LOOKAHEAD_DAYS = 120
HOLIDAY_MARGIN_DAYS = 14

Transform = Callable[[list[dict[str, Any]], dict[date, MarketDay]], list[CalendarEvent]]


@dataclass(frozen=True)
class EventSource:
    source: str
    endpoint: KsdEndpoint
    transform: Transform


SOURCES: dict[str, EventSource] = {
    "dividends": EventSource(SOURCE_DIVIDEND, DIVIDEND, to_dividend_events),
    "bonus": EventSource(SOURCE_BONUS, BONUS_ISSUE, lambda rows, days: to_bonus_events(rows)),
    "rights": EventSource(SOURCE_RIGHTS, RIGHTS_ISSUE, lambda rows, days: to_rights_events(rows)),
    "agm": EventSource(SOURCE_AGM, SHAREHOLDER_MEETING, lambda rows, days: to_agm_events(rows)),
}


def _active_tickers() -> list[str]:
    with session_scope() as session:
        return select_active_tickers(session)


def run(source_name: str, today: date | None = None, client: KisClient | None = None) -> int:
    spec = SOURCES[source_name]
    today = today or now_kst().date()
    start = today - timedelta(days=LOOKBACK_DAYS)
    end = today + timedelta(days=LOOKAHEAD_DAYS)

    with session_scope() as session:
        days = {
            day.trade_date: day
            for day in select_market_days(session, start - timedelta(days=HOLIDAY_MARGIN_DAYS), end)
        }

    tickers = _active_tickers()
    listed = set(tickers)
    stats = FetchStats()
    rows = fetch_ksd_rows(
        spec.endpoint, start, end, lambda: tickers, client=client or get_kis_client(), stats=stats
    )
    events = [event for event in spec.transform(rows, days) if event.ticker in listed]

    with session_scope() as session:
        existing = count_events(session, spec.source, start, end)
        if not events and existing:
            logger.warning(
                "%s: 응답 0건이라 기존 %d행을 유지한다 (%s~%s)",
                spec.source,
                existing,
                start,
                end,
            )
            return 0
        written = replace_events(
            session, spec.source, start, end, events, keep_basis_dates=stats.failed_dates
        )

    if stats.failed_dates:
        logger.warning(
            "%s: 종목별 조회 실패로 기준일 %s는 기존 행을 유지한다 — %s",
            spec.source,
            sorted(set(stats.failed_dates)),
            stats.errors,
        )
    if stats.truncated:
        logger.warning(
            "%s: 한 종목·하루가 한 페이지를 넘어 받은 부분만 적재했다 — %s",
            spec.source,
            stats.truncated,
        )
    logger.info(
        "%s: 구간 %s~%s, 호출 %d회, 넘침 %s, 적재 %d행",
        spec.source,
        start,
        end,
        stats.calls,
        stats.overflow_dates,
        written,
    )
    return written


def run_dividends() -> int:
    return run("dividends")


def run_bonus() -> int:
    return run("bonus")


def run_rights() -> int:
    return run("rights")


def run_agm() -> int:
    return run("agm")
