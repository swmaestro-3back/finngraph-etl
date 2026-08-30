"""트리플 파이프라인의 RDB 적재 (relation_sources · news_companies).

relation_sources는 뉴스·공시 근거를 함께 담는 원장이고, 이 모듈은 그중 뉴스
(source_type='news') 행을 쓴다. 공시 행은 disclosures/loaders/postgres.py가 쓴다.
Neo4j 간선은 이 원장의 집계(entities_relations 뷰)를 캐시한 파생이다 —
그래프 쓰기는 loaders/neo4j.py.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

INSERT_RELATION_SOURCE_SQL = text(
    """
    INSERT INTO relation_sources (
        source_type, news_id,
        subject_name, subject_type, subject_code,
        relation,
        object_name, object_type, object_code,
        evidence, mentioned_at, item,
        source_sentence, polarity, tense,
        subject_impact, object_impact
    )
    VALUES (
        'news', :news_id,
        :subject_name, :subject_type, :subject_code,
        :relation,
        :object_name, :object_type, :object_code,
        :evidence, :mentioned_at, :item,
        :source_sentence, :polarity, :tense,
        :subject_impact, :object_impact
    )
    ON CONFLICT ON CONSTRAINT uq_relsrc_news DO NOTHING;
    """
)


def insert_relation_sources(news_id: int, mentioned_at: date, rows: list[dict]) -> int:
    """뉴스 근거 행을 relation_sources에 멱등 적재한다.

    rows 항목 형식은 edges.source_row_of 반환값. UNIQUE(news_id, 삼중항)로
    재추출 시 ON CONFLICT DO NOTHING. 신규 삽입된 행 수를 반환한다.
    """

    if not rows:
        return 0

    inserted_count = 0

    with session_scope() as session:
        for row in rows:
            result = session.execute(
                INSERT_RELATION_SOURCE_SQL,
                {"news_id": news_id, "mentioned_at": mentioned_at, **row},
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
