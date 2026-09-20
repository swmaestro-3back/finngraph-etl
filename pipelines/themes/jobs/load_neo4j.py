"""task: load_neo4j — 병합 결과를 Neo4j Theme 에 증분 반영한다.

`load_postgres` 다음에 실행된다. Theme.theme_id 가 Postgres themes.id 이므로 RDB 적재가
끝나야 id 가 확정된다(companies_load_us 의 company_id 와 같은 방식). 태스크는 분리되어
있어 재시도 단위는 여전히 저장소별이다.
기존 Theme 은 삭제하지 않는다. 신규 테마는 노드째 추가되고, 기존 테마는 신규 편입
종목의 BELONGS_TO 간선만 추가된다(RDB 쪽 load_themes 와 같은 전략). 마지막에 RDB 전체
테마로 기존 노드의 theme_id 를 백필한다.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.loaders.neo4j import backfill_theme_ids, upsert_themes
from pipelines.themes.models import Theme
from pipelines.themes.references.postgres import fetch_theme_ids

logger = get_logger(__name__)


async def _run(merged_path: str) -> dict:
    raw = json.loads(Path(merged_path).read_text(encoding="utf-8"))
    themes = [Theme(**item) for item in raw]
    theme_ids = fetch_theme_ids()

    async with neo4j_database:
        result = await upsert_themes(themes, theme_ids)
        result["backfilled"] = await backfill_theme_ids(theme_ids)
        return result


def run(merged_path: str) -> None:
    result = asyncio.run(_run(merged_path))
    logger.info("Neo4j 적재 완료: %s, result=%s", merged_path, result)
