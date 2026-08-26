"""트리플 파이프라인의 RDB 적재 (relation_source · news_companies).

Neo4j 적재는 loaders/neo4j.py. 이 모듈은 그래프 적재 결과(간선 elementId)와
기업 매핑을 RDB에 기록해, 그래프 간선 ↔ 출처 뉴스 ↔ 근거 문장을 RDB에서
양방향 조회할 수 있게 한다.
"""

from __future__ import annotations

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope


def insert_relation_sources(news_id: int, rows: list[dict[str, str]]) -> int:
    """Neo4j 간선 출처를 relation_source에 멱등 적재한다.

    rows 항목 형식: {"neo4j_id": 간선 elementId, "reason": 근거 문장(evidence)}.
    UNIQUE(news_id, neo4j_id)로 재추출 시 ON CONFLICT DO NOTHING.
    신규 삽입된 행 수를 반환한다.
    """

    if not rows:
        return 0

    inserted_count = 0

    with session_scope() as session:
        for row in rows:
            result = session.execute(
                text(
                    """
                    INSERT INTO relation_source (news_id, neo4j_id, reason)
                    VALUES (:news_id, :neo4j_id, :reason)
                    ON CONFLICT (news_id, neo4j_id) DO NOTHING;
                    """
                ),
                {
                    "news_id": news_id,
                    "neo4j_id": row["neo4j_id"],
                    "reason": row.get("reason"),
                },
            )
            inserted_count += result.rowcount or 0

    return inserted_count


def insert_news_companies(news_id: int, company_ids: list[int]) -> int:
    """뉴스-기업 매핑을 news_companies에 멱등 적재한다. 신규 행 수를 반환한다."""

    unique_ids = sorted({int(company_id) for company_id in company_ids if company_id})

    if not unique_ids:
        return 0

    inserted_count = 0

    with session_scope() as session:
        for company_id in unique_ids:
            result = session.execute(
                text(
                    """
                    INSERT INTO news_companies (news_id, company_id)
                    VALUES (:news_id, :company_id)
                    ON CONFLICT (news_id, company_id) DO NOTHING;
                    """
                ),
                {"news_id": news_id, "company_id": company_id},
            )
            inserted_count += result.rowcount or 0

    return inserted_count
