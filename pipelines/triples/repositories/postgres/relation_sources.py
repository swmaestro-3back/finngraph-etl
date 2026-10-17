"""relation_sources 원장의 뉴스(source_type='news') 행 적재.

relation_sources는 뉴스·공시 근거를 함께 담는 원장이다. 공시 행은
disclosures/repositories/postgres/relation_sources.py가 쓴다. Neo4j 간선은 이 원장의
집계(entities_relations 뷰)를 캐시한 파생이다 — 그래프 쓰기는
repositories/neo4j/relations.py.
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
