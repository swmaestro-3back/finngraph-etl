"""Neo4j 적재.

Theme 노드와 Company-[:BELONGS_TO]->Theme 간선을 쓴다. 읽기(참조 조회)는
`references/graph.py`에 있다.

Postgres 적재(`loaders/postgres.py`)와 달리 여기에는 트랜잭션 경계가 없다 —
Neo4j 드라이버의 `execute_query`는 쿼리 단위로 커밋한다. 중간에 실패하면 앞선 쿼리는
이미 반영된 상태이므로, 파이프라인은 `reset()` 후 전량 재적재로 복구한다.
"""

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


async def update_theme_embeddings(rows: list[dict[str, Any]]) -> None:
    """(name, embedding) 목록을 Theme 노드에 기록한다.

    원천은 Postgres(themes.embedding)다 — 여기 값은 GraphRAG 조회용 복사본이라
    언제든 pg 에서 전량 재작성할 수 있다. setNodeVectorProperty 는 float32 로
    저장해 프로퍼티 크기를 절반으로 줄인다.
    """

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
