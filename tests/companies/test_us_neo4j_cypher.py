from __future__ import annotations

import pytest

from pipelines.companies.loaders.neo4j import build_us_upsert_cypher


def test_nasdaq_cypher_sets_nasdaq_and_removes_nyse() -> None:
    cypher = build_us_upsert_cypher("NASDAQ")

    assert "MERGE (c:Company {name: row.name})" in cypher
    assert "SET c:NASDAQ" in cypher
    assert "REMOVE c:NYSE" in cypher
    assert "c.sp500 = row.sp500" in cypher
    assert "c.en_name = row.en_name" in cypher
    # KR 라벨은 건드리지 않는다
    assert "KOSPI" not in cypher and "KOSDAQ" not in cypher


def test_nyse_cypher_sets_nyse_and_removes_nasdaq() -> None:
    cypher = build_us_upsert_cypher("NYSE")

    assert "SET c:NYSE" in cypher
    assert "REMOVE c:NASDAQ" in cypher


def test_unknown_market_rejected() -> None:
    with pytest.raises(ValueError):
        build_us_upsert_cypher("KOSPI")
