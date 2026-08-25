"""
companies를 원천으로 상장사만 Neo4j Company 노드로 시드한다.

비상장 법인은 넣지 않는다 — 상장사와 동명인 비상장 법인이 586건 있어, 넣으면
트리플의 name 기반 MERGE가 동명 노드 전부에 간선을 붙이는 모호성이 생긴다.
상장사끼리는 이름 중복이 없으므로 name을 MERGE 키로 쓸 수 있고(name 유니크 제약과도
일치), 뉴스 트리플이 이름만으로 만들어 둔 노드도 같은 MERGE로 흡수된다.
"""

from __future__ import annotations

from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

MIN_ROWS_SAFETY = 1


# [1] 사명 변경 — 노드를 새로 만들지 않고 name만 교체
# 예) 한국조선해양(009540)이 HD한국조선해양으로 사명 변경
#   전  (:Company {name: "한국조선해양", ticker: "009540"})-[:SUPPLIES]->(:Company)
#   후  (:Company {name: "HD한국조선해양", ticker: "009540"})-[:SUPPLIES]->(:Company)
#
# 사명이 바뀌어도 유지되는 ticker로 기존 노드를 찾는다. upsert보다 먼저 와야 한다 —
# 뒤집히면 upsert의 name MERGE가 새 이름으로 별도 노드를 만든다.
RENAME_COMPANIES_CYPHER = """
UNWIND $rows AS row
MATCH (old:Company {ticker: row.ticker})
WHERE old.name <> row.name
SET old.name = row.name
"""

# [2] 신규 상장 · 속성 부여 — MERGE 하나가 두 케이스를 모두 처리
# 예1) 신규 상장 — 그래프에 노드 자체가 없는 경우
#   전  (없음)
#   후  (:Company {name: "두산로보틱스", ticker: "454910", corp_code: "01605286", is_listed: true})
#
# 예2) 속성 부여 — 뉴스에서 트리플이 이름만으로 만들어 둔 노드
#   전  (:Company {name: "삼성전자"})-[:SUPPLIES]->(:Company {name: "애플"})
#   후  (:Company {name: "삼성전자", ticker: "005930", ...})-[:SUPPLIES]->(:Company)
#       이 단계가 없으면 트리플 code 조회(references/graph.py)가 계속 NULL로 남는다.
#
# 상장 여부 판별은 is_listed 속성으로 한다. 시장 라벨(:KOSPI/:KOSDAQ)은 원천이
# companies로 바뀌며 market 정보가 없어져 더는 붙이지 않는다.
UPSERT_COMPANIES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Company {name: row.name})
SET c.company_id = row.company_id,
    c.ticker = row.ticker,
    c.corp_code = row.corp_code,
    c.is_listed = row.is_listed,
    c.country = row.country
"""

# [3] 상장폐지 — 이번 배치에 없는 ticker 보유 노드를 제거하고 간선까지 제거
# 예) 009540이 이번 배치에 없음(상장폐지)
#   전  (:Company {name: "...", ticker: "009540"})-[:SUPPLIES]->(:Company {name: "..."})
#   후  (없음)  — 노드와 간선 모두 삭제, 상대편 노드는 남는다.
#
# ticker가 없는 노드(트리플이 이름만으로 만든 것)는 건드리지 않는다.
DELETE_DELISTED_CYPHER = """
MATCH (c:Company)
WHERE c.ticker IS NOT NULL AND NOT c.ticker IN $tickers
DETACH DELETE c
"""

COUNT_SEEDED_CYPHER = "MATCH (c:Company) WHERE c.ticker IS NOT NULL RETURN count(c) AS seeded"


async def seed_graph_companies(rows: list[dict[str, Any]]) -> int:
    """상장 법인 (company_id, name, ticker, corp_code, is_listed, country)를 Neo4j
    Company 노드에 upsert하고 시드 수를 반환한다.

    is_listed=false(비상장·상장폐지) 행은 여기서 걸러 넣지 않는다. 사명 변경은 같은
    ticker 노드의 name을 바꿔 이력을 승계하고, 상장폐지는 노드를 삭제한다.
    """

    listed = [row for row in rows if row["is_listed"] and row["ticker"]]

    if len(listed) < MIN_ROWS_SAFETY:
        logger.error("시드 원천(companies)에 상장 법인이 없어 중단합니다(전량 삭제 방지).")
        return 0

    tickers = [row["ticker"] for row in listed]

    await neo4j_database.execute(RENAME_COMPANIES_CYPHER, {"rows": listed})
    await neo4j_database.execute(UPSERT_COMPANIES_CYPHER, {"rows": listed})
    await neo4j_database.execute(DELETE_DELISTED_CYPHER, {"tickers": tickers})

    records = await neo4j_database.execute(COUNT_SEEDED_CYPHER)
    seeded = records[0]["seeded"] if records else 0

    logger.info(
        "Neo4j 상장사 시드 완료: 대상 %d개, 그래프 내 ticker 보유 %d개", len(listed), seeded
    )

    return seeded
