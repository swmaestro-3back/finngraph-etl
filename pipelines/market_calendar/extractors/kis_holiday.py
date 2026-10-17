from __future__ import annotations

from datetime import date
from typing import Any

from pipelines.common.clients.kis import KisClient, get_kis_client
from pipelines.market_calendar.models import MarketDay
from pipelines.market_calendar.transformers.parsing import parse_date

HOLIDAY_PATH = "/uapi/domestic-stock/v1/quotations/chk-holiday"
HOLIDAY_TR_ID = "CTCA0903R"


def parse_market_day(row: dict[str, Any]) -> MarketDay | None:
    trade_date = parse_date(row.get("bass_dt"))
    if trade_date is None:
        return None
    return MarketDay(
        trade_date=trade_date,
        is_open=row.get("opnd_yn") == "Y",
        is_business_day=row.get("bzdy_yn") == "Y",
        is_settlement_day=row.get("sttl_day_yn") == "Y",
        weekday_code=str(row.get("wday_dvsn_cd") or "").strip(),
    )


def fetch_market_days(start: date, pages: int, client: KisClient | None = None) -> list[MarketDay]:
    client = client or get_kis_client()
    params = {"BASS_DT": start.strftime("%Y%m%d"), "CTX_AREA_NK": "", "CTX_AREA_FK": ""}
    tr_cont = ""
    days: dict[date, MarketDay] = {}

    for _ in range(pages):
        page = client.request_page(HOLIDAY_PATH, HOLIDAY_TR_ID, params, tr_cont=tr_cont)
        for row in page.data.get("output") or []:
            day = parse_market_day(row)
            if day is not None:
                days[day.trade_date] = day
        if page.tr_cont not in ("F", "M"):
            break
        params = {
            **params,
            "CTX_AREA_NK": str(page.data.get("ctx_area_nk", "")),
            "CTX_AREA_FK": str(page.data.get("ctx_area_fk", "")),
        }
        tr_cont = "N"

    return sorted(days.values(), key=lambda day: day.trade_date)
