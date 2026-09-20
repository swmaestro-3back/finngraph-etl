"""Neo4j 적재."""

from __future__ import annotations

from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme

logger = get_logger(__name__)


def build_theme_batch(
    themes: list[Theme], theme_ids: dict[str, int]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Neo4j 적재 배치를 만든다. Postgres id 가 없는 테마(RDB 적재 실패분)는 제외한다.

    (배치, 제외된 테마명) 을 반환한다. 두 저장소의 정합성을 위해 RDB 에 없는 테마는
    Neo4j 에도 넣지 않는다.
    """

    batch: list[dict[str, Any]] = []
    skipped: list[str] = []

    for theme in themes:
        theme_id = theme_ids.get(theme.name)
        if theme_id is None:
            skipped.append(theme.name)
            continue

        batch.append(
            {
                "name": theme.name,
                "theme_id": theme_id,
                "description": theme.description,
                "source": theme.source,
                "sources": theme.sources,
                "companies": [{"ticker": c.ticker, "reason": c.reason} for c in theme.companies],
            }
        )

    return batch, skipped


async def upsert_themes(themes: list[Theme], theme_ids: dict[str, int]) -> dict[str, Any]:
    """Theme 노드와 BELONGS_TO 간선을 추가한다.

    theme_id 는 Postgres themes.id 다(Company.company_id 와 같은 관례). 그래서 이 함수는
    load_postgres 가 끝난 뒤에 불려야 하고, RDB 에 없는 테마는 건너뛴다. 기존 노드도
    ON MATCH 에서 theme_id 를 채워 백필한다.

    기존 Theme(merge_themes 의 merge_existing 이 DB 쪽 이름으로 맞춰 보낸 것)은 sources 만
    합집합으로 늘리고 description 은 그대로 둔다. 간선은 ON CREATE 만 있어 기존 편입
    종목의 reason 과 reason_embedding 은 건드리지 않고, 신규 편입 종목만 추가된다.
    """

    batch, skipped = build_theme_batch(themes, theme_ids)

    if skipped:
        logger.warning(
            "Postgres id 가 없어 Neo4j 적재를 건너뛴 테마 %d개: %s", len(skipped), skipped
        )

    if batch:
        await neo4j_database.execute(
            """
UNWIND $batch AS theme
MERGE (t:Theme {name: theme.name})
ON CREATE SET
    t.theme_id = theme.theme_id,
    t.description = theme.description,
    t.source = theme.source,
    t.sources = theme.sources
ON MATCH SET
    t.theme_id = theme.theme_id,
    t.sources = t.sources + [s IN theme.sources WHERE NOT s IN t.sources]
WITH t, theme
UNWIND theme.companies AS company
MATCH (c:Company {ticker: company.ticker})
MERGE (c)-[r:BELONGS_TO]->(t)
ON CREATE SET r.reason = company.reason
""",
            parameters={"batch": batch},
        )

    logger.info("%d개 Theme 및 BELONGS_TO 관계 적재 완료 (건너뜀 %d개)", len(batch), len(skipped))
    return {"themes": len(batch), "skipped": len(skipped)}


async def backfill_theme_ids(theme_ids: dict[str, int]) -> int:
    """Postgres 의 전체 (name, id) 로 기존 Theme 노드의 theme_id 를 채운다.

    upsert_themes 는 이번 배치에 포함된 테마만 백필하므로, 더 이상 크롤링되지 않는 옛
    테마까지 채우려면 이 단계가 필요하다. 이미 같은 값이면 no-op 이다.
    """

    rows = [{"name": name, "theme_id": theme_id} for name, theme_id in theme_ids.items()]
    if not rows:
        return 0

    records = await neo4j_database.execute(
        """
UNWIND $batch AS row
MATCH (t:Theme {name: row.name})
WHERE t.theme_id IS NULL OR t.theme_id <> row.theme_id
SET t.theme_id = row.theme_id
RETURN count(t) AS updated
""",
        parameters={"batch": rows},
    )
    updated = int(records[0]["updated"]) if records else 0
    logger.info("Theme.theme_id 백필 %d개", updated)
    return updated


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
