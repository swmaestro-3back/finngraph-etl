from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.market_calendar.models import (
    CompanyRef,
    IpoFilingHead,
    KsdOfferingRef,
    LinkTarget,
    OfferingTerms,
    StoredFiling,
)

UPSERT_FILING_HEAD_SQL = text(
    """
    INSERT INTO ipo_filings (
      corp_code, corp_name, corp_cls, status, spac, first_rcept_no, first_filed_on,
      latest_rcept_no, latest_report_nm, updated_at
    )
    VALUES (
      :corp_code, :corp_name, :corp_cls, :status, :spac, :first_rcept_no, :first_filed_on,
      :latest_rcept_no, :latest_report_nm, now()
    )
    ON CONFLICT (corp_code) DO UPDATE SET
      corp_name = EXCLUDED.corp_name,
      corp_cls = EXCLUDED.corp_cls,
      status = EXCLUDED.status,
      spac = EXCLUDED.spac,
      first_rcept_no = EXCLUDED.first_rcept_no,
      first_filed_on = EXCLUDED.first_filed_on,
      latest_rcept_no = EXCLUDED.latest_rcept_no,
      latest_report_nm = EXCLUDED.latest_report_nm,
      updated_at = now()
    """
)

UPDATE_FILING_TERMS_SQL = text(
    """
    UPDATE ipo_filings
       SET subscr_start = :subscr_start,
           subscr_end = :subscr_end,
           pay_date = :pay_date,
           offer_price = :offer_price,
           offer_shares = :offer_shares,
           offer_amount = :offer_amount,
           offer_method = :offer_method,
           underwriters = CAST(:underwriters AS jsonb),
           fund_uses = CAST(:fund_uses AS jsonb),
           sellers = CAST(:sellers AS jsonb),
           putback = CAST(:putback AS jsonb),
           updated_at = now()
     WHERE corp_code = :corp_code
    """
)

MARK_DESCRIPTION_READY_SQL = text(
    """
    UPDATE ipo_filings
       SET description_ready = true,
           updated_at = now()
     WHERE corp_code = :corp_code
    """
)

UPDATE_FILING_TICKER_SQL = text(
    """
    UPDATE ipo_filings
       SET ticker = :ticker,
           updated_at = now()
     WHERE corp_code = :corp_code
    """
)

SELECT_FILING_STATES_SQL = text(
    """
    SELECT corp_code, first_rcept_no, first_filed_on, latest_rcept_no,
           subscr_start IS NOT NULL AS has_terms, description_ready
      FROM ipo_filings
     WHERE corp_code = ANY(CAST(:codes AS text[]))
    """
)

SELECT_COMPANY_REFS_SQL = text(
    """
    SELECT corp_code, id,
           (ceo_name IS NOT NULL OR established_on IS NOT NULL OR address IS NOT NULL)
             AS has_profile,
           COALESCE(description, '') <> '' AS has_description
      FROM companies
     WHERE corp_code = ANY(CAST(:codes AS text[]))
    """
)

SELECT_LINK_TARGETS_SQL = text(
    """
    SELECT corp_code, corp_name, subscr_start, ticker
      FROM ipo_filings
     ORDER BY corp_code
    """
)

SELECT_KSD_OFFERING_REFS_SQL = text(
    """
    SELECT ticker, name, subscr_start
      FROM ipo_offerings
    """
)

SELECT_LISTED_TICKERS_SQL = text(
    """
    SELECT DISTINCT ON (c.corp_code) c.corp_code, s.ticker
      FROM companies AS c
      JOIN stocks AS s ON s.company_id = c.id AND s.is_active
     WHERE c.corp_code = ANY(CAST(:codes AS text[]))
     ORDER BY c.corp_code, s.ticker
    """
)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def upsert_filing_head(session: Session, head: IpoFilingHead) -> int:
    result = session.execute(
        UPSERT_FILING_HEAD_SQL,
        {
            "corp_code": head.corp_code,
            "corp_name": head.corp_name,
            "corp_cls": head.corp_cls,
            "status": head.status,
            "spac": head.spac,
            "first_rcept_no": head.first_rcept_no,
            "first_filed_on": head.first_filed_on,
            "latest_rcept_no": head.latest_rcept_no,
            "latest_report_nm": head.latest_report_nm,
        },
    )
    return result.rowcount or 0


def update_filing_terms(session: Session, corp_code: str, terms: OfferingTerms) -> int:
    result = session.execute(
        UPDATE_FILING_TERMS_SQL,
        {
            "corp_code": corp_code,
            "subscr_start": terms.subscr_start,
            "subscr_end": terms.subscr_end,
            "pay_date": terms.pay_date,
            "offer_price": terms.offer_price,
            "offer_shares": terms.offer_shares,
            "offer_amount": terms.offer_amount,
            "offer_method": terms.offer_method,
            "underwriters": _json(terms.underwriters),
            "fund_uses": _json(terms.fund_uses),
            "sellers": _json(terms.sellers),
            "putback": None if terms.putback is None else _json(terms.putback),
        },
    )
    return result.rowcount or 0


def mark_description_ready(session: Session, corp_code: str) -> int:
    result = session.execute(MARK_DESCRIPTION_READY_SQL, {"corp_code": corp_code})
    return result.rowcount or 0


def update_filing_tickers(session: Session, changes: Mapping[str, str]) -> int:
    if not changes:
        return 0
    session.execute(
        UPDATE_FILING_TICKER_SQL,
        [{"corp_code": corp_code, "ticker": ticker} for corp_code, ticker in changes.items()],
    )
    return len(changes)


def select_filing_states(session: Session, codes: Sequence[str]) -> dict[str, StoredFiling]:
    if not codes:
        return {}
    rows = session.execute(SELECT_FILING_STATES_SQL, {"codes": list(codes)})
    return {
        row.corp_code: StoredFiling(
            corp_code=row.corp_code,
            first_rcept_no=row.first_rcept_no,
            first_filed_on=row.first_filed_on,
            latest_rcept_no=row.latest_rcept_no,
            has_terms=row.has_terms,
            description_ready=row.description_ready,
        )
        for row in rows
    }


def select_company_refs(session: Session, codes: Sequence[str]) -> dict[str, CompanyRef]:
    if not codes:
        return {}
    rows = session.execute(SELECT_COMPANY_REFS_SQL, {"codes": list(codes)})
    return {
        row.corp_code: CompanyRef(
            corp_code=row.corp_code,
            company_id=row.id,
            has_profile=row.has_profile,
            has_description=row.has_description,
        )
        for row in rows
    }


def select_link_targets(session: Session) -> list[LinkTarget]:
    return [
        LinkTarget(
            corp_code=row.corp_code,
            corp_name=row.corp_name,
            subscr_start=row.subscr_start,
            ticker=row.ticker,
        )
        for row in session.execute(SELECT_LINK_TARGETS_SQL)
    ]


def select_ksd_offering_refs(session: Session) -> list[KsdOfferingRef]:
    return [
        KsdOfferingRef(ticker=row.ticker, name=row.name, subscr_start=row.subscr_start)
        for row in session.execute(SELECT_KSD_OFFERING_REFS_SQL)
    ]


def select_listed_tickers(session: Session, codes: Sequence[str]) -> dict[str, str]:
    if not codes:
        return {}
    rows = session.execute(SELECT_LISTED_TICKERS_SQL, {"codes": list(codes)})
    return {row.corp_code: row.ticker for row in rows}
