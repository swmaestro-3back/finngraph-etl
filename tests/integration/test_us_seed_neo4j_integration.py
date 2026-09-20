"""seed_graph_us_companies Neo4j 통합 테스트 — 라벨·멱등·사명 변경·KR 시드와의 격리.

로컬 Neo4j 필요: docker compose up -d neo4j neo4j-init 후 `pytest -m integration`.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.config import get_settings
from pipelines.companies.loaders.neo4j import seed_graph_companies, seed_graph_us_companies

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not get_settings().neo4j_uri, reason="NEO4J_URI 미설정(CI 등) — 로컬 Neo4j 필요"
    ),
]


def _row(marker: str, **overrides) -> dict:
    base = {
        "company_id": 999999,
        "name": f"파이테스트US{marker}",
        "ticker": f"PYT{marker[:4].upper()}",
        "market": "NASDAQ",
        "en_name": "Pytest US Inc.",
    }
    return {**base, **overrides}


def _run(coro):
    async def _inner():
        async with neo4j_database:
            return await coro()

    return asyncio.run(_inner())


@pytest.fixture
def marker() -> str:
    m = uuid.uuid4().hex[:8]
    yield m

    async def _purge():
        await neo4j_database.execute(
            "MATCH (c:Company) WHERE c.name STARTS WITH $prefix DETACH DELETE c",
            {"prefix": f"파이테스트US{m}"},
        )

    _run(_purge)


async def _labels(name: str) -> list[str]:
    records = await neo4j_database.execute(
        "MATCH (c:Company {name: $name}) RETURN labels(c) AS labels, c.sp500 AS sp500",
        {"name": name},
    )
    return sorted(records[0]["labels"]) if records else []


def test_upsert_is_idempotent_and_sets_market_label(marker: str) -> None:
    row = _row(marker)

    async def _go():
        await seed_graph_us_companies([row], set())
        await seed_graph_us_companies([row], set())
        count = await neo4j_database.execute(
            "MATCH (c:Company {ticker: $t}) RETURN count(c) AS n", {"t": row["ticker"]}
        )
        return count[0]["n"], await _labels(row["name"])

    n, labels = _run(_go)
    assert n == 1
    assert labels == ["Company", "NASDAQ"]


def test_market_transfer_swaps_label(marker: str) -> None:
    row = _row(marker)

    async def _go():
        await seed_graph_us_companies([row], set())
        await seed_graph_us_companies([{**row, "market": "NYSE"}], set())
        return await _labels(row["name"])

    assert _run(_go) == ["Company", "NYSE"]


def test_rename_keeps_node_by_ticker(marker: str) -> None:
    row = _row(marker)
    renamed = {**row, "name": f"파이테스트US{marker}-신명"}

    async def _go():
        await seed_graph_us_companies([row], set())
        await seed_graph_us_companies([renamed], set())
        return await neo4j_database.execute(
            "MATCH (c:Company {ticker: $t}) RETURN count(c) AS n, collect(c.name) AS names",
            {"t": row["ticker"]},
        )

    [rec] = _run(_go)
    assert rec["n"] == 1
    assert rec["names"] == [renamed["name"]]


def test_sp500_is_recomputed_every_run(marker: str) -> None:
    """sp500은 Neo4j 전용 속성이다 — 매 회차 넘겨받은 집합으로 다시 계산된다."""

    row = _row(marker)

    async def _go():
        await seed_graph_us_companies([row], {row["ticker"]})
        after_in = await neo4j_database.execute(
            "MATCH (c:Company {ticker: $t}) RETURN c.sp500 AS sp500, count(c) AS n",
            {"t": row["ticker"]},
        )
        await seed_graph_us_companies([row], set())
        after_out = await neo4j_database.execute(
            "MATCH (c:Company {ticker: $t}) RETURN c.sp500 AS sp500, count(c) AS n",
            {"t": row["ticker"]},
        )
        return after_in[0], after_out[0]

    in_rec, out_rec = _run(_go)
    assert in_rec == {"sp500": True, "n": 1}
    assert out_rec == {"sp500": False, "n": 1}


@pytest.mark.skip(reason="로컬 전용 — KR 노드 전량 삭제 위험; 빈 Neo4j에서만 수동 실행")
def test_kr_seed_does_not_delete_us_nodes(marker: str) -> None:
    """KR 시드의 DELETE_DELISTED_CYPHER를 실제로 실행한다.

    로컬 Neo4j에 실데이터 KR 노드가 있으면 그 노드들이 $tickers=['999999']에 없어
    삭제된다. 이 테스트는 비어 있는 로컬 Neo4j에서만 수동으로 스킵을 풀고 실행할 것.
    """
    us = _row(marker)
    kr = {
        "company_id": 999998,
        "name": f"파이테스트US{marker}-KR",
        "ticker": "999999",
        "corp_code": None,
        "is_listed": True,
        "country": "KR",
        "market": "KOSPI",
        "krx100": False,
        "krx300": False,
        "kosdaq150": False,
    }

    async def _go():
        await seed_graph_us_companies([us], set())
        # KR 시드의 $tickers에 US 티커가 없어도 NYSE/NASDAQ 노드는 삭제 대상에서 빠져야 한다
        await seed_graph_companies([kr])
        return await _labels(us["name"])

    assert _run(_go) == ["Company", "NASDAQ"]
