"""Postgres 적재. rcept_no UNIQUE 가 수집 재개 상태 그 자체다.

fetch_supply_edges → upsert_relation_sources 가 disclosures(원천)를 근거 원장
(relation_sources)의 disclosure 행으로 옮긴다. Neo4j 간선은 원장 집계
(entities_relations 뷰)의 캐시라 그래프 쓰기가 이 도메인에 없다 — 간선 요약 동기화는
triples/loaders/neo4j.py 의 sync_edge_summaries 를 job 에서 함께 쓴다.
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

# 근거 원장 대상 — 제출사·계약상대 양쪽 모두 ticker 가 있는 공시만. 계약상대 ticker 는
# 역매칭 성공(상장사 확정)을 뜻한다. 정규명은 companies 조인으로 해석한다 — Neo4j 시드
# 노드의 name 과 같은 원천(companies.name)이라 간선 자연키가 그래프와 일치한다.
SELECT_SUPPLY_EDGES_SQL = text(
    """
    SELECT d.rcept_no,
           d.rcept_dt,
           COALESCE(d.contract_name, d.contract_type) AS item,
           d.ticker,
           f.name AS filer_name,
           d.counterparty_ticker,
           c.name AS counterparty_name
      FROM disclosures d
      JOIN companies f ON f.ticker = d.ticker AND f.delisted_at IS NULL
      JOIN companies c ON c.ticker = d.counterparty_ticker AND c.delisted_at IS NULL
     WHERE d.ticker IS NOT NULL
       AND d.counterparty_ticker IS NOT NULL
     ORDER BY d.rcept_no
    """
)

# 원장의 disclosure 간선 키 전량 — 재적재 전에 읽어 두면, 이번 원천에서 빠진 간선까지
# 요약 동기화 대상에 넣어 그래프의 남은 값을 바로잡을 수 있다.
SELECT_DISCLOSURE_EDGE_KEYS_SQL = text(
    """
    SELECT DISTINCT subject_name, relation, object_name
      FROM relation_sources
     WHERE source_type = 'disclosure'
    """
)

# 근거 원장(disclosure 행) upsert. 원천(disclosures)이 정정으로 갱신될 수 있어
# DO NOTHING 이 아니라 내용을 덮어쓴다 — 매 실행 전량 재적재가 원장과 원천을 맞춘다.
# polarity/tense 는 상수 — 공시는 체결된 계약의 확정 사실이다.
UPSERT_RELATION_SOURCE_SQL = text(
    """
    INSERT INTO relation_sources (
        source_type, rcept_no,
        subject_name, subject_code, relation, object_name, object_code,
        mentioned_at, item, polarity, tense
    )
    VALUES (
        'disclosure', :rcept_no,
        :filer_name, :filer_ticker, 'SUPPLIES_TO', :counterparty_name, :counterparty_ticker,
        :rcept_dt, :item, 'affirmed', 'past_or_present_fact'
    )
    ON CONFLICT ON CONSTRAINT uq_relsrc_disclosure DO UPDATE SET
        subject_code = EXCLUDED.subject_code,
        object_code = EXCLUDED.object_code,
        mentioned_at = EXCLUDED.mentioned_at,
        item = EXCLUDED.item,
        polarity = EXCLUDED.polarity,
        tense = EXCLUDED.tense
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
    """근거 원장 재료 전량 — 계약상대 ticker 매칭에 성공한 공시."""

    rows = session.execute(SELECT_SUPPLY_EDGES_SQL)
    return [
        SupplyContractEdge(
            rcept_no=row.rcept_no,
            rcept_dt=row.rcept_dt,
            item=row.item,
            filer_ticker=row.ticker,
            filer_name=row.filer_name,
            counterparty_ticker=row.counterparty_ticker,
            counterparty_name=row.counterparty_name,
        )
        for row in rows
    ]


# 이번 원천에 없는 disclosure 행 제거 — 정정으로 계약상대가 바뀌거나 상폐로 매칭에서
# 빠진 공시의 옛 행이 남아 집계를 부풀리는 것을 막는다. upsert 와 합쳐야 전량 재적재다.
DELETE_STALE_RELATION_SOURCES_SQL = text(
    """
    DELETE FROM relation_sources
     WHERE source_type = 'disclosure'
       AND (rcept_no, subject_name, object_name) NOT IN (
           SELECT * FROM unnest(
               CAST(:rcept_nos     AS text[]),
               CAST(:subject_names AS text[]),
               CAST(:object_names  AS text[])
           ))
    """
)


def fetch_disclosure_edge_keys(session: Session) -> list[tuple[str, str, str]]:
    """원장에 현재 실려 있는 disclosure 간선 키 전량."""

    rows = session.execute(SELECT_DISCLOSURE_EDGE_KEYS_SQL)
    return [(row.subject_name, row.relation, row.object_name) for row in rows]


def delete_stale_relation_sources(session: Session, edges: list[SupplyContractEdge]) -> int:
    """이번 원천(edges)에 없는 disclosure 원장 행을 삭제한다. 삭제한 행 수를 반환한다."""

    # NOT IN (빈 unnest)는 모든 행에 참이라 빈 입력이 원장 전량 삭제가 된다.
    # 전량 회수는 의도된 경로가 아니므로 no-op 으로 막는다.
    if not edges:
        return 0

    result = session.execute(
        DELETE_STALE_RELATION_SOURCES_SQL,
        {
            "rcept_nos": [edge.rcept_no for edge in edges],
            "subject_names": [edge.filer_name for edge in edges],
            "object_names": [edge.counterparty_name for edge in edges],
        },
    )
    return result.rowcount or 0


def upsert_relation_sources(session: Session, edges: list[SupplyContractEdge]) -> int:
    """공시 근거 행을 relation_sources에 적재한다. 적재 시도한 행 수를 반환한다."""

    if not edges:
        return 0

    session.execute(
        UPSERT_RELATION_SOURCE_SQL,
        [
            {
                "rcept_no": edge.rcept_no,
                "filer_name": edge.filer_name,
                "filer_ticker": edge.filer_ticker,
                "counterparty_name": edge.counterparty_name,
                "counterparty_ticker": edge.counterparty_ticker,
                "rcept_dt": edge.rcept_dt,
                "item": edge.item,
            }
            for edge in edges
        ],
    )
    return len(edges)
