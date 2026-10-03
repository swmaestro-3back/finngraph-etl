"""투자자 수급 조회·적재."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.stocks.models import InvestorFlow
from pipelines.stocks.repositories.postgres.stocks import fetch_active_stock_ids

UPSERT_INVESTOR_FLOW_SQL = text(
    """
    INSERT INTO stock_investor_flows (
      stock_id, trade_date, individual_net_qty, institution_net_qty,
      foreign_net_qty, foreign_hold_ratio, updated_at
    )
    VALUES (
      :stock_id, :trade_date, :individual_net_qty, :institution_net_qty,
      :foreign_net_qty, :foreign_hold_ratio, now()
    )
    ON CONFLICT (stock_id, trade_date) DO UPDATE SET
      individual_net_qty = EXCLUDED.individual_net_qty,
      institution_net_qty = EXCLUDED.institution_net_qty,
      foreign_net_qty = EXCLUDED.foreign_net_qty,
      foreign_hold_ratio = EXCLUDED.foreign_hold_ratio,
      updated_at = now()
    """
)

SELECT_INVESTOR_FLOW_COUNTS_SQL = text(
    "SELECT stock_id, COUNT(*) AS row_count FROM stock_investor_flows GROUP BY stock_id"
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
            "individual_net_qty": flow.individual_net_qty,
            "institution_net_qty": flow.institution_net_qty,
            "foreign_net_qty": flow.foreign_net_qty,
            "foreign_hold_ratio": flow.foreign_hold_ratio,
        }
        for flow in flows
        if flow.ticker in stock_ids
    ]
    if not payload:
        return 0

    session.execute(UPSERT_INVESTOR_FLOW_SQL, payload)
    return len(payload)


def fetch_investor_flow_counts(session: Session) -> dict[int, int]:
    """종목별 수급 적재 건수(stock_id → 행 수).

    백필 재개 판정용이다 — 이미 목표 분량이 있는 종목은 API 호출 없이 건너뛴다.
    """

    return {row.stock_id: row.row_count for row in session.execute(SELECT_INVESTOR_FLOW_COUNTS_SQL)}
