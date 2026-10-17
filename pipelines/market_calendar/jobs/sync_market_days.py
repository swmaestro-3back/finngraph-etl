from __future__ import annotations

from datetime import date

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.market_calendar.extractors.kis_holiday import fetch_market_days
from pipelines.market_calendar.repositories.postgres.market_days import upsert_market_days

logger = get_logger(__name__)

DEFAULT_PAGES = 5


def run(
    pages: int = DEFAULT_PAGES, start: date | None = None, client: KisClient | None = None
) -> int:
    start = start or now_kst().date()
    days = fetch_market_days(start, pages, client=client or get_kis_client())
    with session_scope() as session:
        written = upsert_market_days(session, days)
    logger.info("휴장일 동기화: %s부터 %d일", start, written)
    return written
