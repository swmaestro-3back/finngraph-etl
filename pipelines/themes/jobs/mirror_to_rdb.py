from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database
from pipelines.news.loaders.search_keyword_repository import (
    generate_theme_search_keywords,
)
from pipelines.themes.loaders.rdb_mirror import mirror_themes_to_rdb

logger = get_logger(__name__)

# Neo4j 최종 상태(validate/merge 완료본)를 읽는다. 소스별 raw JSON이 아니라 그래프를 읽어야
# 병합·기업검증 결과가 반영된다. 종목이 없는 테마도 유지(빈 stocks)한다.
_READ_THEMES_QUERY = """
MATCH (t:Theme)
OPTIONAL MATCH (c:Company)-[r:BELONGS_TO]->(t)
WITH t, collect(
    CASE WHEN c:KOSPI OR c:KOSDAQ THEN {ticker: c.ticker, reason: r.reason} END
) AS raw_stocks
RETURN t.name AS name,
       coalesce(t.description, '') AS description,
       coalesce(t.source, '') AS source,
       [s IN raw_stocks WHERE s IS NOT NULL] AS stocks
"""


async def _fetch_themes_from_neo4j() -> list[dict[str, Any]]:
    records = await neo4j_database.execute(_READ_THEMES_QUERY)

    themes: list[dict[str, Any]] = []

    for record in records:
        stocks = [
            {"ticker": stock.get("ticker"), "reason": stock.get("reason")}
            for stock in (record["stocks"] or [])
            if stock and stock.get("ticker")
        ]

        themes.append(
            {
                "name": record["name"],
                "description": record["description"],
                "source": record["source"],
                "stocks": stocks,
            }
        )

    return themes


async def _run_async() -> list[dict[str, Any]]:
    neo4j_database.init_driver()

    try:
        return await _fetch_themes_from_neo4j()
    finally:
        await neo4j_database.close()


def run() -> None:
    themes = asyncio.run(_run_async())
    logger.info("Neo4j에서 테마 %d개 조회", len(themes))

    result = mirror_themes_to_rdb(themes)

    # 미러로 themes.id가 확정된 직후, 테마명에서 검색 키워드를 파생 생성한다(source_id=theme_id).
    # 크롤러가 새로 발견한 테마도 자동으로 검색 대상이 되고, news_themes 매핑 선행조건이 성립한다.
    keyword_count = generate_theme_search_keywords()

    print("\n" + "=" * 60)
    print("테마 RDB 미러 결과")
    print("=" * 60)
    print(f"- Neo4j 조회 테마 {len(themes)}개")
    print(f"- 적재 결과 {result}")
    print(f"- 테마 키워드 생성/갱신 {keyword_count}개")
    print("작업 완료")
    print("=" * 60)


if __name__ == "__main__":
    run()
