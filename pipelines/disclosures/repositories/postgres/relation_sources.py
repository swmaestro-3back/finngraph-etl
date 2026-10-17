"""relation_sources 원장의 공시(source_type='disclosure') 행 조회·적재.

뉴스 행은 triples/repositories/postgres/relation_sources.py 가 쓴다. Neo4j 간선은 원장
집계(entities_relations 뷰)의 캐시라 그래프 쓰기가 이 도메인에 없다 — 간선 요약 동기화는
triples/repositories/neo4j/relations.py 의 sync_edge_summaries 를 job 에서 함께 쓴다.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.disclosures.models import SupplyContractEdge

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
# polarity/tense/impact 는 상수 — 공시는 체결된 공급계약의 확정 사실이므로
# 공급사(subject)에 호재, 수요사(object)에 중립이다.
UPSERT_RELATION_SOURCE_SQL = text(
    """
    INSERT INTO relation_sources (
        source_type, rcept_no,
        subject_name, subject_code, relation, object_name, object_code,
        mentioned_at, item, polarity, tense, subject_impact, object_impact
    )
    VALUES (
        'disclosure', :rcept_no,
        :filer_name, :filer_ticker, 'SUPPLIES_TO', :counterparty_name, :counterparty_ticker,
        :rcept_dt, :item, 'affirmed', 'past_or_present_fact', 'positive', 'neutral'
    )
    ON CONFLICT ON CONSTRAINT uq_relsrc_disclosure DO UPDATE SET
        subject_code = EXCLUDED.subject_code,
        object_code = EXCLUDED.object_code,
        mentioned_at = EXCLUDED.mentioned_at,
        item = EXCLUDED.item,
        polarity = EXCLUDED.polarity,
        tense = EXCLUDED.tense,
        subject_impact = EXCLUDED.subject_impact,
        object_impact = EXCLUDED.object_impact
    """
)

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
