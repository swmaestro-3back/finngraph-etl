"""ipo_offerings 테이블 조회·적재."""

from __future__ import annotations

import json
from collections.abc import Collection
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import (
    IpoOffering,
    KsdOfferingRef,
)

COUNT_IPOS_SQL = text("SELECT COUNT(*) FROM ipo_offerings WHERE basis_date BETWEEN :start AND :end")

DELETE_IPOS_SQL = text(
    """
    DELETE FROM ipo_offerings
     WHERE basis_date BETWEEN :start AND :end
       AND NOT (basis_date = ANY(CAST(:keep AS date[])))
    """
)

INSERT_IPO_SQL = text(
    """
    INSERT INTO ipo_offerings (
      ticker, name, subscr_start, subscr_end, offer_price, pay_date, refund_date,
      listing_date, lead_managers, basis_date, detail, updated_at
    )
    VALUES (
      :ticker, :name, :subscr_start, :subscr_end, :offer_price, :pay_date, :refund_date,
      :listing_date, :lead_managers, :basis_date, CAST(:detail AS jsonb), now()
    )
    ON CONFLICT (ticker, subscr_start) DO UPDATE SET
      name = EXCLUDED.name,
      subscr_end = EXCLUDED.subscr_end,
      offer_price = EXCLUDED.offer_price,
      pay_date = EXCLUDED.pay_date,
      refund_date = EXCLUDED.refund_date,
      listing_date = EXCLUDED.listing_date,
      lead_managers = EXCLUDED.lead_managers,
      basis_date = EXCLUDED.basis_date,
      detail = EXCLUDED.detail,
      updated_at = now()
    """
)


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def count_ipo_offerings(session: Session, start: date, end: date) -> int:
    return int(session.execute(COUNT_IPOS_SQL, {"start": start, "end": end}).scalar_one())


def replace_ipo_offerings(
    session: Session,
    start: date,
    end: date,
    offerings: list[IpoOffering],
    keep_basis_dates: Collection[date] = (),
) -> int:
    keep = sorted(set(keep_basis_dates))
    unique = {
        (offering.ticker, offering.subscr_start): offering
        for offering in offerings
        if start <= offering.basis_date <= end and offering.basis_date not in keep
    }
    session.execute(DELETE_IPOS_SQL, {"start": start, "end": end, "keep": keep})
    if unique:
        session.execute(
            INSERT_IPO_SQL,
            [
                {
                    "ticker": offering.ticker,
                    "name": offering.name,
                    "subscr_start": offering.subscr_start,
                    "subscr_end": offering.subscr_end,
                    "offer_price": offering.offer_price,
                    "pay_date": offering.pay_date,
                    "refund_date": offering.refund_date,
                    "listing_date": offering.listing_date,
                    "lead_managers": offering.lead_managers,
                    "basis_date": offering.basis_date,
                    "detail": _json(offering.detail),
                }
                for offering in unique.values()
            ],
        )
    return len(unique)


SELECT_KSD_OFFERING_REFS_SQL = text(
    """
    SELECT ticker, name, subscr_start
      FROM ipo_offerings
    """
)


def select_ksd_offering_refs(session: Session) -> list[KsdOfferingRef]:
    return [
        KsdOfferingRef(ticker=row.ticker, name=row.name, subscr_start=row.subscr_start)
        for row in session.execute(SELECT_KSD_OFFERING_REFS_SQL)
    ]
