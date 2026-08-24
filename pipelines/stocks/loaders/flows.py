"""투자자 수급·배당 적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.stocks.loaders.tickers import fetch_active_stock_ids
from pipelines.stocks.models import Dividend, InvestorFlow

UPSERT_INVESTOR_FLOW_SQL = text(
    """
    INSERT INTO investor_flows (
      stock_id, trade_date, foreign_net, personal_net, institution_net,
      pension_net, trust_net, insurance_net, bank_net, etc_corp_net,
      foreign_ratio, updated_at
    )
    VALUES (
      :stock_id, :trade_date, :foreign_net, :personal_net, :institution_net,
      :pension_net, :trust_net, :insurance_net, :bank_net, :etc_corp_net,
      :foreign_ratio, now()
    )
    ON CONFLICT (stock_id, trade_date) DO UPDATE SET
      foreign_net = EXCLUDED.foreign_net,
      personal_net = EXCLUDED.personal_net,
      institution_net = EXCLUDED.institution_net,
      pension_net = EXCLUDED.pension_net,
      trust_net = EXCLUDED.trust_net,
      insurance_net = EXCLUDED.insurance_net,
      bank_net = EXCLUDED.bank_net,
      etc_corp_net = EXCLUDED.etc_corp_net,
      -- 보유는 최신 거래일 행에만 실려 온다. 매 회차 30일을 다시 upsert하므로 그대로
      -- 덮으면 어제 채운 값이 NULL로 지워진다.
      foreign_ratio = COALESCE(
        EXCLUDED.foreign_ratio, investor_flows.foreign_ratio
      ),
      updated_at = now()
    """
)

UPSERT_DIVIDEND_SQL = text(
    """
    INSERT INTO dividends (listing_id, record_date, divi_kind, dps, pay_date, updated_at)
    VALUES (:stock_id, :record_date, :divi_kind, :dps, :pay_date, now())
    ON CONFLICT (listing_id, record_date, divi_kind) DO UPDATE SET
      dps = EXCLUDED.dps,
      pay_date = EXCLUDED.pay_date,
      updated_at = now()
    """
)


def upsert_investor_flows(session: Session, flows: list[InvestorFlow]) -> int:
    """투자자 수급을 적재한다.

    Returns:
        int: 적재한 행 수. stock_id를 찾지 못한 종목은 제외된다.
    """

    if not flows:
        return 0

    stock_ids = fetch_active_stock_ids(session)
    payload = [
        {
            "stock_id": stock_ids[flow.ticker],
            "trade_date": flow.trade_date,
            "foreign_net": flow.foreign_net,
            "personal_net": flow.personal_net,
            "institution_net": flow.institution_net,
            "pension_net": flow.pension_net,
            "trust_net": flow.trust_net,
            "insurance_net": flow.insurance_net,
            "bank_net": flow.bank_net,
            "etc_corp_net": flow.etc_corp_net,
            "foreign_ratio": flow.foreign_ratio,
        }
        for flow in flows
        if flow.ticker in stock_ids
    ]
    if not payload:
        return 0

    session.execute(UPSERT_INVESTOR_FLOW_SQL, payload)
    return len(payload)


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
