"""task: embed_themes — 테마 설명·편입 사유를 임베딩해 Neo4j에 기록한다."""

from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.bedrock import embed_texts
from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.themes.loaders.embeddings import EMBEDDING_DIM, theme_text
from pipelines.themes.loaders.neo4j import (
    fetch_reason_embedding_targets,
    fetch_theme_embedding_targets,
    update_reason_embeddings,
    update_theme_embeddings,
)

logger = get_logger(__name__)

EMBED_BATCH = 100


async def _embed_and_store(targets: list[dict[str, Any]], update_fn) -> int:
    """대상을 청크로 임베딩해 기록한다. 청크 단위로 커밋된다."""

    count = 0
    for chunk in chunked(targets, EMBED_BATCH):
        vectors = embed_texts([row["text"] for row in chunk], dim=EMBEDDING_DIM)
        await update_fn(
            [{**row, "embedding": vector} for row, vector in zip(chunk, vectors, strict=True)]
        )
        count += len(chunk)
    return count


async def _run() -> tuple[int, int]:
    async with neo4j_database:
        theme_rows = await fetch_theme_embedding_targets()
        theme_targets = [
            {
                "name": row["name"],
                "text": theme_text(row["name"], row["description"]),
            }
            for row in theme_rows
        ]
        theme_count = await _embed_and_store(theme_targets, update_theme_embeddings)

        reason_rows = await fetch_reason_embedding_targets()
        reason_targets = [
            {
                "ticker": row["ticker"],
                "theme_name": row["theme_name"],
                "text": row["reason"],
            }
            for row in reason_rows
        ]
        reason_count = await _embed_and_store(reason_targets, update_reason_embeddings)

    return theme_count, reason_count


def run() -> None:
    theme_count, reason_count = asyncio.run(_run())
    logger.info(
        "테마 임베딩 완료: 테마 노드 %d개, BELONGS_TO 편입사유 간선 %d개",
        theme_count,
        reason_count,
    )
