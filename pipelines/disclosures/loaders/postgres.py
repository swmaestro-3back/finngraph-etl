"""Postgres 적재. rcept_no UNIQUE 가 수집 재개 상태 그 자체다.

그래프(Neo4j) 쓰기는 `loaders/neo4j.py`에 있다. 여기의 fetch_supply_edges 는 그
그래프 적재의 입력을 만드는 조회다 — disclosures 테이블이 원천이고 그래프는 파생이라,
간선은 언제든 이 조회 결과로 전량 재구성할 수 있다.
"""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.disclosures.models import DisclosureRecord, SupplyContractEdge

UPSERT_DISCLOSURE_SQL = text(
    """
    INSERT INTO disclosures (
      rcept_no, corp_code, company_id, ticker, corp_cls, report_nm,
      is_correction, rcept_dt, flr_nm, link,
      contract_type, contract_name, counterparty,
      counterparty_corp_name, counterparty_corp_code, counterparty_ticker,
      start_date, end_date, order_date,
      fields, meta, created_at, updated_at
    )
    VALUES (
      :rcept_no, :corp_code, :company_id, :ticker, :corp_cls, :report_nm,
      :is_correction, :rcept_dt, :flr_nm, :link,
      :contract_type, :contract_name, :counterparty,
      :counterparty_corp_name, :counterparty_corp_code, :counterparty_ticker,
      :start_date, :end_date, :order_date,
      CAST(:fields AS jsonb), CAST(:meta AS jsonb), now(), now()
    )
    ON CONFLICT (rcept_no) DO UPDATE SET
      company_id = EXCLUDED.company_id,
      ticker = EXCLUDED.ticker,
      corp_cls = EXCLUDED.corp_cls,
      report_nm = EXCLUDED.report_nm,
      is_correction = EXCLUDED.is_correction,
      rcept_dt = EXCLUDED.rcept_dt,
      flr_nm = EXCLUDED.flr_nm,
      link = EXCLUDED.link,
      contract_type = EXCLUDED.contract_type,
      contract_name = EXCLUDED.contract_name,
      counterparty = EXCLUDED.counterparty,
      counterparty_corp_name = EXCLUDED.counterparty_corp_name,
      counterparty_corp_code = EXCLUDED.counterparty_corp_code,
      counterparty_ticker = EXCLUDED.counterparty_ticker,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date,
      order_date = EXCLUDED.order_date,
      fields = EXCLUDED.fields,
      meta = EXCLUDED.meta,
      updated_at = now()
    """
)

SELECT_EXISTING_RCEPT_NOS_SQL = text(
    "SELECT rcept_no FROM disclosures WHERE rcept_no = ANY(:rcept_nos)"
)

# 그래프 간선 대상 — 제출사·계약상대 양쪽 모두 ticker 가 있는 공시만. 계약상대 ticker 는
# 역매칭 성공(상장사 확정)을 뜻하고, 그래프의 Company 노드가 ticker 로 식별되기 때문이다.
SELECT_SUPPLY_EDGES_SQL = text(
    """
    SELECT rcept_no, link, ticker, counterparty_ticker
      FROM disclosures
     WHERE ticker IS NOT NULL
       AND counterparty_ticker IS NOT NULL
     ORDER BY rcept_no
    """
)


def upsert_disclosures(session: Session, records: list[DisclosureRecord]) -> int:
    """공시 행을 적재한다.

    Returns:
        int: 적재 시도한 행 수.
    """

    if not records:
        return 0

    session.execute(
        UPSERT_DISCLOSURE_SQL,
        [
            {
                "rcept_no": record.rcept_no,
                "corp_code": record.corp_code,
                "company_id": record.company_id,
                "ticker": record.ticker,
                "corp_cls": record.corp_cls,
                "report_nm": record.report_nm,
                "is_correction": record.is_correction,
                "rcept_dt": record.rcept_dt,
                "flr_nm": record.flr_nm,
                "link": record.link,
                "contract_type": record.contract_type,
                "contract_name": record.contract_name,
                "counterparty": record.counterparty,
                "counterparty_corp_name": record.counterparty_corp_name,
                "counterparty_corp_code": record.counterparty_corp_code,
                "counterparty_ticker": record.counterparty_ticker,
                "start_date": record.start_date,
                "end_date": record.end_date,
                "order_date": record.order_date,
                "fields": json.dumps(record.fields, ensure_ascii=False),
                "meta": json.dumps(record.meta, ensure_ascii=False),
            }
            for record in records
        ],
    )
    return len(records)


def fetch_existing_rcept_nos(session: Session, rcept_nos: list[str]) -> set[str]:
    """이미 적재된 접수번호. 이 차집합만 원문을 받는다 — 로컬 인덱스 파일의 대체다."""

    if not rcept_nos:
        return set()
    rows = session.execute(SELECT_EXISTING_RCEPT_NOS_SQL, {"rcept_nos": rcept_nos})
    return {row.rcept_no for row in rows}


def fetch_supply_edges(session: Session) -> list[SupplyContractEdge]:
    """그래프 간선 재료 전량 — 계약상대 ticker 매칭에 성공한 공시."""

    rows = session.execute(SELECT_SUPPLY_EDGES_SQL)
    return [
        SupplyContractEdge(
            rcept_no=row.rcept_no,
            link=row.link,
            filer_ticker=row.ticker,
            counterparty_ticker=row.counterparty_ticker,
        )
        for row in rows
    ]
