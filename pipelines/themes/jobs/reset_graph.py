"""task: reset_graph — 적재 전 기존 Theme 노드와 간선을 비운다."""

from __future__ import annotations

import asyncio

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.loaders.neo4j import delete_all_themes

logger = get_logger(__name__)


async def _run() -> None:
    async with neo4j_database:
        await delete_all_themes()
        logger.info("기존 Theme 및 연결 간선 삭제 완료")


def run() -> None:
    asyncio.run(_run())
