"""Neo4j 참조 데이터 조회.

검증(`transformers.validator`)은 크롤링 결과를 이미 그래프에 있는 Company·Theme와
대조해야 한다. 그 조회를 `loaders/`에 두면 transformer가 loader를 임포트하게 되어
E→T→L 방향이 깨지고, `extractors/`에 두면 외부 소스 크롤러와 섞인다 — 여기 쓰이는
쿼리는 파이프라인 입력이 아니라 검증에 쓰는 **참조**다.

`references/`는 우리 저장소를 읽기만 한다. 쓰기(upsert·삭제)는 `loaders/neo4j.py`다.
"""

from __future__ import annotations

from pipelines.common.clients.neo4j import neo4j_database


async def theme_exists() -> bool:
    """Theme 노드가 하나라도 있는지. 최초 적재인지 판별해 병합 조회를 건너뛰는 데 쓴다."""

    records = await neo4j_database.execute(
        """
MATCH (t:Theme)
RETURN t LIMIT 1
"""
    )
    return len(records) > 0


async def fetch_theme_stock_map() -> dict[str, set[str]]:
    """테마명 → 소속 종목 ticker 집합. 기존 테마와의 중복 판정에 쓴다."""

    records = await neo4j_database.execute(
        """
MATCH (t:Theme)
OPTIONAL MATCH (c:Company)-[:BELONGS_TO]->(t)
WHERE c:KOSPI OR c:KOSDAQ
RETURN t.name AS theme_name, coalesce(t.description, '') AS description,
       collect(c.ticker) AS tickers
"""
    )
    return {r["theme_name"]: set(r["tickers"]) for r in records}


async def fetch_company_ticker_by_names(names: list[str]) -> dict[str, str]:
    """기업명 → ticker. 크롤링한 ticker가 옳은지 대조하는 데 쓴다."""

    records = await neo4j_database.execute(
        """
UNWIND $names AS name
MATCH (c:Company {name: name})
WHERE c:KOSPI OR c:KOSDAQ
RETURN c.name AS name, c.ticker AS ticker
""",
        parameters={"names": names},
    )
    return {r["name"]: r["ticker"] for r in records}
