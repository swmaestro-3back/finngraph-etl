"""배당 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.stocks.models import Dividend
from pipelines.stocks.repositories.postgres.stocks import fetch_active_stock_ids

UPSERT_DIVIDEND_SQL = text(
    """
    INSERT INTO stock_dividends (listing_id, record_date, divi_kind, dps, pay_date, updated_at)
    VALUES (:stock_id, :record_date, :divi_kind, :dps, :pay_date, now())
    ON CONFLICT (listing_id, record_date, divi_kind) DO UPDATE SET
      dps = EXCLUDED.dps,
      pay_date = EXCLUDED.pay_date,
      updated_at = now()
    """
)


def upsert_dividends(session: Session, dividends: list[Dividend]) -> int:
    """배당을 적재한다."""

    if not dividends:
        return 0

    stock_ids = fetch_active_stock_ids(session)
    payload = [
        {
            "stock_id": stock_ids[dividend.ticker],
            "record_date": dividend.record_date,
            "divi_kind": dividend.divi_kind,
            "dps": dividend.dps,
            "pay_date": dividend.pay_date,
        }
        for dividend in dividends
        if dividend.ticker in stock_ids
    ]
    if not payload:
        return 0

    # 같은 (종목, 기준일, 종류)가 한 응답에 두 번 오면 ON CONFLICT가 같은 행을 두 번
    # 건드려 실패한다. 뒤에 온 값을 남긴다.
    deduped = {
        (row["stock_id"], row["record_date"], row["divi_kind"]): row for row in payload
    }.values()

    session.execute(UPSERT_DIVIDEND_SQL, list(deduped))
    return len(deduped)
