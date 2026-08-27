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
      is_correction, correction_target_report, correction_target_date,
      correction_reason, original_rcept_no, rcept_dt, flr_nm, link,
      contract_type, contract_name, counterparty,
      counterparty_corp_name, counterparty_corp_code, counterparty_ticker,
      start_date, end_date, order_date,
      fields, meta, created_at, updated_at
    )
    VALUES (
      :rcept_no, :corp_code, :company_id, :ticker, :corp_cls, :report_nm,
      :is_correction, :correction_target_report, :correction_target_date,
      :correction_reason, :original_rcept_no, :rcept_dt, :flr_nm, :link,
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
      correction_target_report = EXCLUDED.correction_target_report,
      correction_target_date = EXCLUDED.correction_target_date,
      correction_reason = EXCLUDED.correction_reason,
      -- 재파싱 시 정정공시의 새 값(NULL)이 link job 의 체인 해소 결과를 지우지 않게
      -- 기존 값을 보존한다. 잘못 해소된 값도 다음 link 실행이 전량 재계산으로 바로잡는다.
      original_rcept_no = COALESCE(EXCLUDED.original_rcept_no, disclosures.original_rcept_no),
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

# 정정 체인 해소 — 각 정정공시의 부모(직전 회차)를 같은 회사의 correction_target_date
# 접수분에서 찾고, 재귀로 루트(최초 원공시)까지 따라가 original_rcept_no 를 확정한다.
#
# 부모 후보가 여럿이면(같은 날 같은 회사의 공시 다건) 접수번호가 앞서는 것 중 가장 늦은
# 행을 직전 회차로 본다 — DART 의 "정정관련 공시서류제출일"이 직전 회차의 제출일이라
# 접수 순서상 가장 가까운 앞선 행이 맞을 확률이 가장 높다. 같은 날 서로 다른 계약이
# 섞이면 오매칭 여지가 있으나, 매 실행 전량 재계산이라 로직 개선 시 자동 교정된다.
# 부모를 못 찾은 정정(백필 범위 밖·비정형 제출일)은 자기 자신을 루트로 삼아 별개
# 계약으로 취급한다 — 집계를 실제보다 줄이지 않는 보수적 폴백이다.
RESOLVE_ORIGINAL_RCEPT_NOS_SQL = text(
    """
    WITH RECURSIVE parents AS (
        SELECT DISTINCT ON (c.rcept_no)
               c.rcept_no,
               p.rcept_no AS parent_rcept_no
          FROM disclosures c
          JOIN disclosures p
            ON p.corp_code = c.corp_code
           AND p.rcept_dt = c.correction_target_date
           AND p.rcept_no < c.rcept_no
         WHERE c.is_correction
           AND c.correction_target_date IS NOT NULL
         ORDER BY c.rcept_no, p.rcept_no DESC
    ),
    roots AS (
        SELECT d.rcept_no, d.rcept_no AS root_rcept_no
          FROM disclosures d
          LEFT JOIN parents pr ON pr.rcept_no = d.rcept_no
         WHERE pr.rcept_no IS NULL
        UNION ALL
        SELECT pr.rcept_no, r.root_rcept_no
          FROM parents pr
          JOIN roots r ON r.rcept_no = pr.parent_rcept_no
    )
    UPDATE disclosures d
       SET original_rcept_no = r.root_rcept_no,
           updated_at = now()
      FROM roots r
     WHERE d.rcept_no = r.rcept_no
       AND d.original_rcept_no IS DISTINCT FROM r.root_rcept_no
    """
)

# 근거 원장 대상 — 체인(original_rcept_no)당 최신 회차 1행만. 같은 계약의 원공시·정정
# 회차들이 원장에 나란히 실려 disclosure_count 를 부풀리는 것을 막는다. 내용(계약상대·
# 계약명)은 최신 회차의 확정값을 쓰고, mentioned_at 은 원공시 접수일이다(계약이 처음
# 알려진 시점). 제출사·계약상대 양쪽 모두 ticker 가 있는 행만 남는다 — 최신 회차가
# 매칭에 실패하면 체인 전체가 빠지고 delete_stale 이 옛 간선을 회수한다. 정규명은
# companies 조인으로 해석한다 — Neo4j 시드 노드의 name 과 같은 원천(companies.name)이라
# 간선 자연키가 그래프와 일치한다.
SELECT_SUPPLY_EDGES_SQL = text(
    """
    WITH latest AS (
        SELECT DISTINCT ON (COALESCE(original_rcept_no, rcept_no))
               rcept_no,
               COALESCE(original_rcept_no, rcept_no) AS original_rcept_no,
               contract_name, contract_type, ticker, counterparty_ticker
          FROM disclosures
         ORDER BY COALESCE(original_rcept_no, rcept_no), rcept_no DESC
    )
    SELECT l.rcept_no,
           o.rcept_dt,
           COALESCE(l.contract_name, l.contract_type) AS item,
           l.ticker,
           f.name AS filer_name,
           l.counterparty_ticker,
           c.name AS counterparty_name
      FROM latest l
      JOIN disclosures o ON o.rcept_no = l.original_rcept_no
      JOIN companies f ON f.ticker = l.ticker AND f.delisted_at IS NULL
      JOIN companies c ON c.ticker = l.counterparty_ticker AND c.delisted_at IS NULL
     WHERE l.ticker IS NOT NULL
       AND l.counterparty_ticker IS NOT NULL
     ORDER BY l.rcept_no
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
                "correction_target_report": record.correction_target_report,
                "correction_target_date": record.correction_target_date,
                "correction_reason": record.correction_reason,
                "original_rcept_no": record.original_rcept_no,
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


def resolve_original_rcept_nos(session: Session) -> int:
    """정정 체인을 재귀로 풀어 original_rcept_no 를 확정한다. 갱신한 행 수를 반환한다.

    매 실행 전량 재계산이라 멱등이다 — 늦게 적재된 원공시도 다음 실행에서 연결되고,
    새 정정이 오면 그 체인의 루트가 다시 계산된다.
    """

    result = session.execute(RESOLVE_ORIGINAL_RCEPT_NOS_SQL)
    return result.rowcount or 0


def fetch_supply_edges(session: Session) -> list[SupplyContractEdge]:
    """근거 원장 재료 — 계약상대 ticker 매칭에 성공한 공시, 체인당 최신 회차 1행."""

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
