"""Neo4j 적재."""

from __future__ import annotations

from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme

logger = get_logger(__name__)


async def upsert_themes(themes: list[Theme]) -> dict[str, Any]:
    """Theme 노드와 BELONGS_TO 간선을 추가한다.

    기존 Theme(merge_themes 의 merge_existing 이 DB 쪽 이름으로 맞춰 보낸 것)은 sources 만
    합집합으로 늘리고 description 은 그대로 둔다. 간선은 ON CREATE 만 있어 기존 편입
    종목의 reason 과 reason_embedding 은 건드리지 않고, 신규 편입 종목만 추가된다.
    """

    batch = [
        {
            "name": theme.name,
            "source_theme_id": theme.source_theme_id,
            "description": theme.description,
            "source": theme.source,
            "sources": theme.sources,
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
    t.source = theme.source,
    t.sources = theme.sources
ON MATCH SET
    t.sources = t.sources + [s IN theme.sources WHERE NOT s IN t.sources]
WITH t, theme
UNWIND theme.companies AS company
MATCH (c:Company {ticker: company.ticker})
MERGE (c)-[r:BELONGS_TO]->(t)
ON CREATE SET r.reason = company.reason
""",
        parameters={"batch": batch},
    )

    logger.info("%d개 Theme 및 BELONGS_TO 관계 적재 완료", len(themes))
    return {"themes": len(themes)}


async def fetch_theme_embedding_targets() -> list[dict[str, Any]]:
    """
    description 임베딩이 아직 안된 테마 조회
    """

    records = await neo4j_database.execute(
        """
MATCH (t:Theme)
WHERE t.embedding IS NULL
RETURN t.name AS name,
       t.description AS description
"""
    )
    return [dict(record) for record in records]


async def fetch_reason_embedding_targets() -> list[dict[str, Any]]:
    """
    reason 임베딩이 아직 안된 BELONGS_TO 간선 조회
    """

    records = await neo4j_database.execute(
        """
MATCH (c:Company)-[r:BELONGS_TO]->(t:Theme)
WHERE r.reason IS NOT NULL
  AND r.reason_embedding IS NULL
RETURN c.ticker AS ticker,
       t.name AS theme_name,
       r.reason AS reason
"""
    )
    return [dict(record) for record in records]


async def update_theme_embeddings(rows: list[dict[str, Any]]) -> None:
    """(name, embedding) 목록을 Theme 노드에 기록한다."""

    if not rows:
        return

    await neo4j_database.execute(
        """
UNWIND $batch AS row
MATCH (t:Theme {name: row.name})
CALL db.create.setNodeVectorProperty(t, 'embedding', row.embedding)
""",
        parameters={"batch": rows},
    )

    logger.info("%d개 Theme 노드에 임베딩 기록 완료", len(rows))


async def update_reason_embeddings(rows: list[dict[str, Any]]) -> None:
    """(ticker, theme_name, embedding) 목록을 BELONGS_TO 간선에 기록한다."""

    if not rows:
        return

    await neo4j_database.execute(
        """
UNWIND $batch AS row
MATCH (c:Company {ticker: row.ticker})-[r:BELONGS_TO]->(t:Theme {name: row.theme_name})
CALL db.create.setRelationshipVectorProperty(r, 'reason_embedding', row.embedding)
""",
        parameters={"batch": rows},
    )

    logger.info("%d개 BELONGS_TO 간선에 편입 사유 임베딩 기록 완료", len(rows))
