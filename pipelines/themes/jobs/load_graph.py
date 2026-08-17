"""task: load_graph — 검증 결과를 Neo4j에 적재한다.

`load_rdb`와 형제 태스크로 병렬 실행된다. 두 저장소는 실패 특성이 달라 재시도 단위를
분리해 둔다 — Postgres 적재가 실패했다고 Neo4j 전량 재적재까지 되돌릴 이유가 없다.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.loaders.neo4j import upsert_themes
from pipelines.themes.models import Theme

logger = get_logger(__name__)


async def _run(validated_path: str) -> None:
    async with neo4j_database:
        raw = json.loads(Path(validated_path).read_text(encoding="utf-8"))
        await upsert_themes([Theme(**item) for item in raw])


def run(validated_path: str) -> None:
    asyncio.run(_run(validated_path))
    logger.info("Neo4j 적재 완료: %s", validated_path)
