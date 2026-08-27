"""task: embed_themes — 테마 설명·편입 사유를 임베딩해 pgvector 컬럼에 적재하고,
테마 임베딩을 Neo4j Theme 노드에도 복사한다 (GraphRAG 벡터 검색 → 1-hop 확장용).

테마 리프레시(load_rdb·load_graph)가 전량 삭제-재적재라 매 회차 전량 재임베딩이다.
청크 단위로 커밋하므로 중간 실패 시 재시도가 이미 적재한 청크(해시 일치)는
건너뛰고 이어간다. Neo4j 동기화는 pg 에 있는 임베딩 전량을 밀어넣는 방식이라
어느 시점에 재시도해도 결과가 같다 — 원천은 항상 pg 다.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from pipelines.common.clients.bedrock import embed_texts
from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.themes.loaders.embeddings import (
    EMBEDDING_DIM,
    fetch_stale_reasons,
    fetch_stale_themes,
    fetch_theme_embeddings,
    update_reason_embeddings,
    update_theme_embeddings,
)
from pipelines.themes.loaders.neo4j import (
    ensure_theme_vector_index,
)
from pipelines.themes.loaders.neo4j import (
    update_theme_embeddings as update_graph_theme_embeddings,
)

logger = get_logger(__name__)

EMBED_BATCH = 100


def _embed_and_store(rows: list[dict[str, Any]], update_fn: Callable[..., int]) -> int:
    count = 0
    for chunk in chunked(rows, EMBED_BATCH):
        vectors = embed_texts([row["text"] for row in chunk], dim=EMBEDDING_DIM)
        payload = [
            {"id": row["id"], "embedding": vector, "text_hash": row["text_hash"]}
            for row, vector in zip(chunk, vectors, strict=True)
        ]
        with session_scope() as session:
            count += update_fn(session, payload)
    return count


async def _sync_graph_embeddings(rows: list[dict[str, Any]]) -> None:
    async with neo4j_database:
        await ensure_theme_vector_index()
        await update_graph_theme_embeddings(rows)


def run() -> None:
    with session_scope() as session:
        stale_themes = fetch_stale_themes(session)
        stale_reasons = fetch_stale_reasons(session)

    # 외부 API 호출 동안 트랜잭션을 잡아두지 않도록 세션 밖에서 임베딩한다.
    theme_count = _embed_and_store(stale_themes, update_theme_embeddings)
    reason_count = _embed_and_store(stale_reasons, update_reason_embeddings)

    # pg 를 원천으로 Neo4j Theme 노드에 임베딩 전량 복사.
    with session_scope() as session:
        graph_rows = fetch_theme_embeddings(session)
    asyncio.run(_sync_graph_embeddings(graph_rows))

    logger.info(
        "테마 임베딩 완료: 테마 %d개, 편입사유 %d개, Neo4j 동기화 %d개",
        theme_count,
        reason_count,
        len(graph_rows),
    )
