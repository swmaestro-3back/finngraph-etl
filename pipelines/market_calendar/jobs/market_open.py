from __future__ import annotations

from datetime import date

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.market_calendar.extractors.kis_holiday import fetch_market_days
from pipelines.market_calendar.loaders.store import select_market_days, upsert_market_days


def is_market_open(target: date, client: KisClient | None = None) -> bool:
    with session_scope() as session:
        known = select_market_days(session, target, target)
    if known:
        return known[0].is_open

    fetched = fetch_market_days(target, pages=1, client=client or get_kis_client())
    with session_scope() as session:
        upsert_market_days(session, fetched)

    for day in fetched:
        if day.trade_date == target:
            return day.is_open
    raise ValueError(f"휴장일 조회 응답에 {target} 이 없다")
