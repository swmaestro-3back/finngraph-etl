"""task: load_neo4j — 병합 결과를 Neo4j Theme 에 증분 반영한다.

`load_postgres`와 형제 태스크로 병렬 실행된다. 두 저장소는 실패 특성이 달라 재시도 단위를
분리해 둔다 — Postgres 적재가 실패했다고 Neo4j 적재까지 되돌릴 이유가 없다.
기존 Theme 은 삭제하지 않는다. 신규 테마는 노드째 추가되고, 기존 테마는 신규 편입
종목의 BELONGS_TO 간선만 추가된다(RDB 쪽 load_themes 와 같은 전략).
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


async def _run(merged_path: str) -> dict:
    async with neo4j_database:
        raw = json.loads(Path(merged_path).read_text(encoding="utf-8"))
        return await upsert_themes([Theme(**item) for item in raw])


def run(merged_path: str) -> None:
    result = asyncio.run(_run(merged_path))
    logger.info("Neo4j 적재 완료: %s, result=%s", merged_path, result)
