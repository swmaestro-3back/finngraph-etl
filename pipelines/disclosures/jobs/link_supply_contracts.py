"""공급계약 공시를 Neo4j 그래프 간선으로 연결한다.

disclosures 테이블에서 계약상대 ticker 매칭에 성공한 공시 전량을 읽어
(제출사)-[:SUPPLIES_TO]->(계약상대) 간선으로 반영한다. 원천이 Postgres 라 매 실행이
전량 재구성이고 멱등이다 — 수집 job 과 분리해 둔 덕에 매칭 로직이 바뀌어도 재수집 없이
이 job 만 다시 돌리면 그래프가 따라온다.
"""

from __future__ import annotations

import asyncio

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.disclosures.loaders.neo4j import upsert_supply_edges
from pipelines.disclosures.loaders.postgres import fetch_supply_edges

logger = get_logger(__name__)


async def _link() -> None:
    with session_scope() as session:
        edges = fetch_supply_edges(session)

    if not edges:
        logger.info("그래프에 연결할 공급계약 공시가 없습니다.")
        return

    async with neo4j_database:
        await upsert_supply_edges(edges)


def run() -> None:
    asyncio.run(_link())
