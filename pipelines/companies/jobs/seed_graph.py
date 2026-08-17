"""task: seed_graph — 활성 보통주를 Neo4j Company 노드로 시드한다.

테마 검증·적재와 삼중항 code 채움이 이 시드를 전제로 한다.

Postgres 읽기는 동기, Neo4j 쓰기는 비동기라 여기서 두 구간을 순차로 잇는다. 세션은
읽기가 끝나면 바로 닫아 Neo4j 왕복 동안 커넥션을 붙들고 있지 않게 한다.
"""

from __future__ import annotations

import asyncio

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.extractors.listed_stocks import fetch_listed_common_stocks
from pipelines.companies.loaders.neo4j import seed_graph_companies

logger = get_logger(__name__)


async def _seed(rows) -> int:
    async with neo4j_database:
        return await seed_graph_companies(rows)


def run() -> None:
    with session_scope() as session:
        rows = fetch_listed_common_stocks(session)

    seeded = asyncio.run(_seed(rows))

    logger.info(
        "Neo4j 상장사 시드 결과: 원천(활성 보통주) %d개, 그래프 내 ticker 보유 %d개",
        len(rows),
        seeded,
    )
