from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database

logger = get_logger(__name__)

MIN_ROWS_SAFETY = 1

# 시드 원천: 활성 보통주만. companies upsert와 같은 필터(수집 범위 ≠ 제공 범위).
SELECT_LISTED_COMMON_STOCKS_SQL = text(
    """
    SELECT s.name, s.ticker, s.market
      FROM stocks AS s
     WHERE s.is_active
       AND NOT s.preferred_stock
       AND NOT s.etp
       AND NOT s.spac
       AND BTRIM(s.name) <> ''
    """
)

# name이 병렬 MERGE·시드의 유일 키이므로 유니크 제약으로 중복 생성을 원천 차단한다.
ENSURE_NAME_CONSTRAINT_CYPHER = """
CREATE CONSTRAINT company_name_uk IF NOT EXISTS
FOR (c:Company) REQUIRE c.name IS UNIQUE
"""

# 사명 변경·코드 재사용 정리: 이번 배치의 ticker를 다른 name 노드가 쥐고 있으면 자격 회수.
# 노드·간선은 남긴다(과거 뉴스 관계 보존) — 상장사 자격(ticker·시장 라벨)만 거둔다.
RECLAIM_RENAMED_CYPHER = """
UNWIND $rows AS row
MATCH (old:Company {ticker: row.ticker})
WHERE old.name <> row.name
REMOVE old.ticker, old:KOSPI, old:KOSDAQ
"""

# 삼중항이 만드는 노드와 같은 키(name)로 MERGE해야 기존 노드에 ticker가 얹힌다.
# 시장 이전(KOSPI↔KOSDAQ)은 새 라벨 부여 + 반대 라벨 제거로 처리한다.
UPSERT_COMPANIES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Company {name: row.name})
SET c.ticker = row.ticker
FOREACH (_ IN CASE WHEN row.market = 'KOSPI' THEN [1] ELSE [] END |
  SET c:KOSPI REMOVE c:KOSDAQ)
FOREACH (_ IN CASE WHEN row.market = 'KOSDAQ' THEN [1] ELSE [] END |
  SET c:KOSDAQ REMOVE c:KOSPI)
"""

# 상장폐지 정리: 이번 배치에 없는 ticker 보유 노드의 자격 회수.
RECLAIM_DELISTED_CYPHER = """
MATCH (c:Company)
WHERE c.ticker IS NOT NULL AND NOT c.ticker IN $tickers
REMOVE c.ticker, c:KOSPI, c:KOSDAQ
"""

COUNT_SEEDED_CYPHER = "MATCH (c:Company) WHERE c.ticker IS NOT NULL RETURN count(c) AS seeded"


def fetch_listed_common_stocks(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(SELECT_LISTED_COMMON_STOCKS_SQL).all()
    return [{"name": row.name, "ticker": row.ticker, "market": row.market} for row in rows]


async def seed_graph_companies(rows: list[dict[str, Any]]) -> int:
    """상장사 (name, ticker, 시장 라벨)를 Neo4j Company 노드에 upsert하고 시드 수를 반환한다.

    삭제 없는 정합 방식 — 노드·간선은 보존하고 상장사 자격만 부여/회수한다.
    """

    if len(rows) < MIN_ROWS_SAFETY:
        logger.error("시드 원천(stocks)이 비어 있어 중단합니다(전량 자격 회수 방지).")
        return 0

    tickers = [row["ticker"] for row in rows]

    await neo4j_database.execute(ENSURE_NAME_CONSTRAINT_CYPHER)
    await neo4j_database.execute(RECLAIM_RENAMED_CYPHER, {"rows": rows})
    await neo4j_database.execute(UPSERT_COMPANIES_CYPHER, {"rows": rows})
    await neo4j_database.execute(RECLAIM_DELISTED_CYPHER, {"tickers": tickers})

    records = await neo4j_database.execute(COUNT_SEEDED_CYPHER)
    seeded = records[0]["seeded"] if records else 0

    logger.info("Neo4j 상장사 시드 완료: 대상 %d개, 그래프 내 ticker 보유 %d개", len(rows), seeded)

    return seeded
