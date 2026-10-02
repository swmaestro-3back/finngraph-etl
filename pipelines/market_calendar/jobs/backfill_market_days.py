from __future__ import annotations

from datetime import date, timedelta

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.market_calendar.extractors.kis_holiday import fetch_market_days
from pipelines.market_calendar.loaders.store import upsert_market_days
from pipelines.market_calendar.models import MarketDay

logger = get_logger(__name__)

ONE_DAY = timedelta(days=1)
DEFAULT_DAYS_BACK = 365


class BackfillGapError(RuntimeError):
    pass


def fetch_range(start: date, end: date, client: KisClient) -> list[MarketDay]:
    cursor = start
    collected: dict[date, MarketDay] = {}
    while cursor <= end:
        days = fetch_market_days(cursor, pages=1, client=client)
        returned = {day.trade_date for day in days}
        if cursor not in returned:
            first = min(returned) if returned else None
            raise BackfillGapError(f"휴장일 API가 {cursor}를 주지 않는다 (응답 첫 날짜 {first})")
        for day in days:
            collected[day.trade_date] = day
        cursor = max(returned) + ONE_DAY
    return sorted(collected.values(), key=lambda day: day.trade_date)


def run(
    days_back: int = DEFAULT_DAYS_BACK, today: date | None = None, client: KisClient | None = None
) -> int:
    today = today or now_kst().date()
    start = today - timedelta(days=days_back)
    days = fetch_range(start, today, client or get_kis_client())
    with session_scope() as session:
        written = upsert_market_days(session, days)
    logger.info("휴장일 백필: %s~%s, %d일", start, today, written)
    return written
