from __future__ import annotations

from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

MIN_ROWS_SAFETY = 1


# [1] 사명 변경 — 노드를 새로 만들지 않고 name만 교체
# 예) 한국조선해양(009540)이 HD한국조선해양으로 사명 변경
#   전  (:Company:KOSPI {name: "한국조선해양", ticker: "009540"})-[:SUPPLIES]->(:Company)
#   후  (:Company:KOSPI {name: "HD한국조선해양", ticker: "009540"})-[:SUPPLIES]->(:Company)
RENAME_COMPANIES_CYPHER = """
UNWIND $rows AS row
MATCH (old:Company {ticker: row.ticker})
WHERE old.name <> row.name
SET old.name = row.name
"""

# [2] 신규 상장 · ticker 부여 · 시장 이전 — MERGE 하나가 세 케이스를 모두 처리
# 예1) 신규 상장 — 그래프에 노드 자체가 없는 경우
#   전  (없음)
#   후  (:Company:KOSPI {name: "두산로보틱스", ticker: "454910"})
#
# 예2) ticker 부여 — 뉴스에서 삼중항이 이름만으로 만들어 둔 노드
#   전  (:Company {name: "삼성전자"})-[:SUPPLIES]->(:Company {name: "애플"})
#   후  (:Company:KOSPI {name: "삼성전자", ticker: "005930"})-[:SUPPLIES]->(:Company)
#       이 단계가 없으면 삼중항 code 조회(references/graph.py)가 계속 NULL로 남는다.
#
# 예3) 시장 이전 — KOSDAQ에서 KOSPI로
#   전  (:Company:KOSDAQ {name: "에코프로비엠", ticker: "247540"})
#   후  (:Company:KOSPI  {name: "에코프로비엠", ticker: "247540"})
#       반대 라벨 REMOVE가 없으면 :KOSPI:KOSDAQ을 동시에 달게 된다.
UPSERT_COMPANIES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Company {name: row.name})
SET c.ticker = row.ticker
FOREACH (_ IN CASE WHEN row.market = 'KOSPI' THEN [1] ELSE [] END |
  SET c:KOSPI REMOVE c:KOSDAQ)
FOREACH (_ IN CASE WHEN row.market = 'KOSDAQ' THEN [1] ELSE [] END |
  SET c:KOSDAQ REMOVE c:KOSPI)
"""

# [3] 상장폐지 — 이번 배치에 없는 ticker 보유 노드를 제거하고 간선까지 제거
# 예) 009540이 이번 배치에 없음(상장폐지)
#   전  (:Company:KOSPI {name: "...", ticker: "009540"})-[:SUPPLIES]->(:Company {name: "..."})
#   후  (없음)  — 노드와 간선 모두 삭제, 상대편 노드는 남는다.
DELETE_DELISTED_CYPHER = """
MATCH (c:Company)
WHERE c.ticker IS NOT NULL AND NOT c.ticker IN $tickers
DETACH DELETE c
"""

COUNT_SEEDED_CYPHER = "MATCH (c:Company) WHERE c.ticker IS NOT NULL RETURN count(c) AS seeded"


async def seed_graph_companies(rows: list[dict[str, Any]]) -> int:
    """상장사 (name, ticker, 시장 라벨)를 Neo4j Company 노드에 upsert하고 시드 수를 반환한다.

    사명 변경은 같은 노드의 name을 바꿔 이력을 승계하고, 상장폐지는 노드를 삭제한다.
    rename이 upsert보다 먼저 와야 한다 — 뒤집히면 upsert가 새 이름으로 별도 노드를 만든다.
    """

    if len(rows) < MIN_ROWS_SAFETY:
        logger.error("시드 원천(stocks)이 비어 있어 중단합니다(전량 삭제 방지).")
        return 0

    tickers = [row["ticker"] for row in rows]

    await neo4j_database.execute(RENAME_COMPANIES_CYPHER, {"rows": rows})
    await neo4j_database.execute(UPSERT_COMPANIES_CYPHER, {"rows": rows})
    await neo4j_database.execute(DELETE_DELISTED_CYPHER, {"tickers": tickers})

    records = await neo4j_database.execute(COUNT_SEEDED_CYPHER)
    seeded = records[0]["seeded"] if records else 0

    logger.info("Neo4j 상장사 시드 완료: 대상 %d개, 그래프 내 ticker 보유 %d개", len(rows), seeded)

    return seeded
