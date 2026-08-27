"""Neo4j 적재."""

from __future__ import annotations

from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme

logger = get_logger(__name__)

# GraphRAG 조회 경로: db.index.vector.queryNodes('theme_embedding', k, $qvec)
# → BELONGS_TO 1-hop 확장. 차원(1024)은 pgvector 스키마와 같은 모델에 결합된 값이라
# execute 의 LiteralString 제약상 쿼리에 직접 박는다.


async def delete_all_themes() -> None:
    """Theme 노드와 여기 붙은 간선을 전부 삭제한다. 전량 재적재 전에 호출한다."""

    await neo4j_database.execute(
        """
MATCH (t:Theme)
DETACH DELETE t
"""
    )


async def upsert_themes(themes: list[Theme]) -> None:
    """Theme 노드와 BELONGS_TO 간선을 적재한다."""

    batch = [
        {
            "name": theme.name,
            "source_theme_id": theme.source_theme_id,
            "description": theme.description,
            "source": theme.source,
            "companies": [{"ticker": c.ticker, "reason": c.reason} for c in theme.companies],
        }
        for theme in themes
    ]

    await neo4j_database.execute(
        """
UNWIND $batch AS theme
MERGE (t:Theme {name: theme.name})
ON CREATE SET
    t.source_theme_id = theme.source_theme_id,
    t.description = theme.description,
    t.source = theme.source
WITH t, theme
UNWIND theme.companies AS company
MATCH (c:Company {ticker: company.ticker})
MERGE (c)-[r:BELONGS_TO]->(t)
ON CREATE SET r.reason = company.reason
""",
        parameters={"batch": batch},
    )

    logger.info("%d개 Theme 및 BELONGS_TO 관계 적재 완료", len(themes))


async def ensure_theme_vector_index() -> None:
    """Theme.embedding 벡터 인덱스를 보장한다. IF NOT EXISTS 라 멱등이다."""

    await neo4j_database.execute(
        """
CREATE VECTOR INDEX theme_embedding IF NOT EXISTS
FOR (t:Theme) ON t.embedding
OPTIONS {indexConfig: {
    `vector.dimensions`: 1024,
    `vector.similarity_function`: 'cosine'
}}
"""
    )


async def ensure_reason_vector_index() -> None:
    """BELONGS_TO.reason_embedding 관계 벡터 인덱스를 보장한다. IF NOT EXISTS 라 멱등이다."""

    await neo4j_database.execute(
        """
CREATE VECTOR INDEX belongs_to_reason_embedding IF NOT EXISTS
FOR ()-[r:BELONGS_TO]-() ON r.reason_embedding
OPTIONS {indexConfig: {
    `vector.dimensions`: 1024,
    `vector.similarity_function`: 'cosine'
}}
"""
    )


async def fetch_theme_embedding_targets() -> list[dict[str, Any]]:
    """전체 Theme 의 임베딩 대상 재료 — 스테일 판정(loaders/embeddings.py)의 입력이다."""

    records = await neo4j_database.execute(
        """
MATCH (t:Theme)
RETURN t.name AS name,
       t.description AS description,
       t.embedding_text_hash AS stored_hash,
       t.embedding IS NOT NULL AS has_embedding
"""
    )
    return [dict(record) for record in records]


async def fetch_reason_embedding_targets() -> list[dict[str, Any]]:
    """reason 이 있는 BELONGS_TO 간선의 임베딩 대상 재료. 간선 키는 (ticker, 테마명)이다."""

    records = await neo4j_database.execute(
        """
MATCH (c:Company)-[r:BELONGS_TO]->(t:Theme)
WHERE r.reason IS NOT NULL
RETURN c.ticker AS ticker,
       t.name AS theme_name,
       r.reason AS reason,
       r.reason_text_hash AS stored_hash,
       r.reason_embedding IS NOT NULL AS has_embedding
"""
    )
    return [dict(record) for record in records]


async def update_theme_embeddings(rows: list[dict[str, Any]]) -> None:
    """(name, embedding, text_hash) 목록을 Theme 노드에 기록한다."""

    if not rows:
        return

    await neo4j_database.execute(
        """
UNWIND $batch AS row
MATCH (t:Theme {name: row.name})
SET t.embedding_text_hash = row.text_hash
WITH t, row
CALL db.create.setNodeVectorProperty(t, 'embedding', row.embedding)
""",
        parameters={"batch": rows},
    )

    logger.info("%d개 Theme 노드에 임베딩 기록 완료", len(rows))


async def update_reason_embeddings(rows: list[dict[str, Any]]) -> None:
    """(ticker, theme_name, embedding, text_hash) 목록을 BELONGS_TO 간선에 기록한다."""

    if not rows:
        return

    await neo4j_database.execute(
        """
UNWIND $batch AS row
MATCH (c:Company {ticker: row.ticker})-[r:BELONGS_TO]->(t:Theme {name: row.theme_name})
SET r.reason_text_hash = row.text_hash
WITH r, row
CALL db.create.setRelationshipVectorProperty(r, 'reason_embedding', row.embedding)
""",
        parameters={"batch": rows},
    )

    logger.info("%d개 BELONGS_TO 간선에 편입 사유 임베딩 기록 완료", len(rows))
