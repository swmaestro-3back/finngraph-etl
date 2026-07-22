from __future__ import annotations

from pipelines.common.neo4j import neo4j_database


# 전체 Theme 노드와 여기 연결된 간선 삭제
async def delete_all_themes() -> None:
    await neo4j_database.execute(
        """
MATCH (t:Theme)
DETACH DELETE t
"""
    )


# 테마 존재성 확인
# 초기 테마가 하나도 없는 경우에 저장하기 위함
async def theme_exists() -> bool:
    records = await neo4j_database.execute(
        """
MATCH (t:Theme)
RETURN t LIMIT 1
"""
    )
    return len(records) > 0


# 전체 테마와 테마주 정보 조회
async def fetch_theme_stock_map() -> dict[str, set[str]]:
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


# 전체 기업 조회
# WHERE 절로 해도 라벨 인덱스를 탄다
async def fetch_existing_company_tickers() -> set[str]:
    records = await neo4j_database.execute(
        """
MATCH (c:Company)
WHERE c:KOSPI OR c:KOSDAQ
RETURN c.ticker AS ticker
"""
    )
    return {r["ticker"] for r in records}


# 기업명에 대한 Ticker 값 조회
async def fetch_company_ticker_by_names(names: list[str]) -> dict[str, str]:
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


# 테마 저장
# 여기엔 굳이 없어도 될거 같은데?
async def upsert_themes(theme_batch: list[dict]) -> None:
    await neo4j_database.execute(
        """
UNWIND $batch AS theme
MERGE (t:Theme {name: theme.name})
ON CREATE SET
    t.id = randomUUID(),
    t.source_theme_id = theme.source_theme_id,
    t.description = theme.description,
    t.source = theme.source
WITH t, theme
UNWIND theme.companies AS company
MATCH (c:Company {ticker: company.ticker})
MERGE (c)-[r:BELONGS_TO]->(t)
ON CREATE SET r.reason = company.reason
""",
        parameters={"batch": theme_batch},
    )
