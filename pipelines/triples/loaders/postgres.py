"""트리플 파이프라인의 RDB 적재 (relation_sources · news_companies · news.triple_extracted).

relation_sources는 뉴스·공시 근거를 함께 담는 원장이고, 이 모듈은 그중 뉴스
(source_type='news') 행을 쓴다. 공시 행은 disclosures/loaders/postgres.py가 쓴다.
Neo4j 간선은 이 원장의 집계(entities_relations 뷰)를 캐시한 파생이다 —
그래프 쓰기는 loaders/neo4j.py.
"""

from __future__ import annotations

from datetime import date
from typing import Any

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


def fetch_unprocessed_triple_news_items(limit: int = 100) -> list[dict[str, Any]]:
    """
    Triple ETL에서 사용.
    삼중항 추출이 아직 시도되지 않은(triple_extracted IS NULL) 뉴스를 조회한다.
    추출 중 예외가 난 뉴스는 NULL로 남아 다음 런에서 자동 재시도된다.
    """

    query = """
        SELECT
            id,
            text,
            (COALESCE(published_at, collected_at, now()))::date AS mentioned_at
        FROM news
        WHERE triple_extracted IS NULL
          AND text IS NOT NULL
          AND BTRIM(text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        return [
            {"news_id": int(news_id), "text": news_text, "mentioned_at": mentioned_at}
            for news_id, news_text, mentioned_at in rows
        ]


def mark_triple_extraction_result(
    has_triples_ids: list[int], no_triples_ids: list[int]
) -> dict[str, int]:
    """
    Triple ETL에서 호출.
    삼중항 추출을 시도한 뉴스의 triple_extracted에 삼중항 존재 여부를 마킹한다.
    (NULL=미시도이므로 TRUE/FALSE 어느 쪽이든 "시도 완료"를 겸한다)
    """

    unique_true = sorted({int(news_id) for news_id in has_triples_ids if news_id})
    unique_false = sorted({int(news_id) for news_id in no_triples_ids if news_id})

    if not unique_true and not unique_false:
        return {"true_count": 0, "false_count": 0}

    with session_scope() as session:
        if unique_true:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET triple_extracted = TRUE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_true},
            )

        if unique_false:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET triple_extracted = FALSE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_false},
            )

    return {"true_count": len(unique_true), "false_count": len(unique_false)}
