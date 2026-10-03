from __future__ import annotations

from datetime import date, timedelta

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.market_calendar.extractors.kis_ksd import PUBLIC_OFFERING, FetchStats, fetch_ksd_rows
from pipelines.market_calendar.loaders.store import (
    count_ipo_offerings,
    replace_ipo_offerings,
    select_active_tickers,
)
from pipelines.market_calendar.transformers.events import to_ipo_offerings

logger = get_logger(__name__)

LOOKBACK_DAYS = 60
LOOKAHEAD_DAYS = 90


def _active_tickers() -> list[str]:
    with session_scope() as session:
        return select_active_tickers(session)


def run(today: date | None = None, client: KisClient | None = None) -> int:
    today = today or now_kst().date()
    start = today - timedelta(days=LOOKBACK_DAYS)
    end = today + timedelta(days=LOOKAHEAD_DAYS)

    stats = FetchStats()
    rows = fetch_ksd_rows(
        PUBLIC_OFFERING, start, end, _active_tickers, client=client or get_kis_client(), stats=stats
    )
    offerings = to_ipo_offerings(rows)

    with session_scope() as session:
        existing = count_ipo_offerings(session, start, end)
        if not offerings and existing:
            logger.warning(
                "공모주: 응답 0건이라 기존 %d행을 유지한다 (%s~%s)",
                existing,
                start,
                end,
            )
            return 0
        written = replace_ipo_offerings(
            session, start, end, offerings, keep_basis_dates=stats.failed_dates
        )

    if stats.failed_dates:
        logger.warning(
            "공모주: 종목별 조회 실패로 기준일 %s는 기존 행을 유지한다 — %s",
            sorted(set(stats.failed_dates)),
            stats.errors,
        )
    logger.info(
        "공모주: 구간 %s~%s, 호출 %d회, 적재 %d행",
        start,
        end,
        stats.calls,
        written,
    )
    return written
