from __future__ import annotations

import asyncio

from pipelines.common.database import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database
from pipelines.companies.loaders.graph_seed import fetch_listed_common_stocks, seed_graph_companies

logger = get_logger(__name__)


async def _run_async(rows) -> int:
    async with neo4j_database:
        return await seed_graph_companies(rows)


def run() -> None:
    """활성 보통주를 Neo4j Company 노드로 시드한다(테마 검증·적재, 삼중항 코드 채움의 전제)."""

    with session_scope() as session:
        rows = fetch_listed_common_stocks(session)

    seeded = asyncio.run(_run_async(rows))

    print("\n" + "=" * 60)
    print("Neo4j 상장사 시드 결과")
    print("=" * 60)
    print(f"- 원천(stocks 활성 보통주) {len(rows)}개, 그래프 내 ticker 보유 {seeded}개")
    print("작업 완료")
    print("=" * 60)


if __name__ == "__main__":
    run()
