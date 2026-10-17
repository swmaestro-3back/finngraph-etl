"""
테마 설명 및 편입 사유를 임베딩하여 저장 (Neo4j 전용)

임베딩이 없는 노드·간선만 대상이라 중간에 실패해도 재실행하면 이어서 채운다. 청크마다
기록하므로 청크는 재시도 시 다시 임베딩할 양이고, 청크 안은 스레드로 병렬 호출한다
(common/clients/bedrock.embed_texts).
"""

from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.bedrock import embed_texts
from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.themes.repositories.neo4j.themes import (
    fetch_reason_embedding_targets,
    fetch_theme_embedding_targets,
    update_reason_embeddings,
    update_theme_embeddings,
)

logger = get_logger(__name__)

# 워커 수(기본 32)의 몇 배로 잡아 청크 끝에서 스레드가 노는 시간을 줄인다.
EMBED_BATCH = 512

EMBEDDING_DIM = 1024


def theme_text(name: str, description: str | None) -> str:

    return f"{name}\n{description or ''}"


def reason_text(theme_name: str, reason: str) -> str:
    # 사유만으론 "주력 생산"처럼 맥락이 빈 문장이 많아 테마명을 앞에 붙인다. 질의 측
    # (finngraph-ai-server)이 근거로 보여주는 "[테마명] 사유" 형식과 같다.
    return f"[{theme_name}] {reason}"


async def _embed_and_store(targets: list[dict[str, Any]], update_fn) -> int:

    count = 0
    for chunk in chunked(targets, EMBED_BATCH):
        # boto3 호출은 동기라 스레드로 넘겨 Neo4j 드라이버의 이벤트 루프를 막지 않는다.
        vectors = await asyncio.to_thread(
            embed_texts, [row["text"] for row in chunk], dim=EMBEDDING_DIM
        )
        await update_fn(
            [{**row, "embedding": vector} for row, vector in zip(chunk, vectors, strict=True)]
        )
        count += len(chunk)
        logger.info("임베딩 진행: %d / %d", count, len(targets))
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
                "text": reason_text(row["theme_name"], row["reason"]),
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
