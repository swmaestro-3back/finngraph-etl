"""task: load_neo4j — Postgres의 US 법인을 Neo4j Company 노드로 시드.

법인 행은 Postgres에서 읽는다. company_id가 여기서 생기고, KR 시드(seed_graph)도 같은
원천을 쓰므로 그래프와 RDB가 어긋나지 않는다. sp500 소속만은 Postgres에 없어 이번 회차
`us.json`에서 직접 다시 계산한다 — Neo4j 전용 속성이라 매 회차 새로 매긴다.
읽기는 동기, 쓰기는 비동기라 순차로 잇는다.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.extractors.listed_stocks import fetch_us_companies
from pipelines.companies.loaders.neo4j import seed_graph_us_companies

logger = get_logger(__name__)


async def _seed(rows, sp500_tickers: set[str]) -> int:
    async with neo4j_database:
        return await seed_graph_us_companies(rows, sp500_tickers)


def run(index_path: str) -> int:
    index_rows = json.loads(Path(index_path).read_text(encoding="utf-8"))
    sp500_tickers = {row["ticker"] for row in index_rows if row["sp500"]}
    logger.info("S&P 500 편입 티커 %d개(us.json 기준)", len(sp500_tickers))

    with session_scope() as session:
        rows = fetch_us_companies(session)

    seeded = asyncio.run(_seed(rows, sp500_tickers))
    logger.info("Neo4j US 시드 결과: 원천 %d개, 그래프 내 %d개", len(rows), seeded)
    return seeded
