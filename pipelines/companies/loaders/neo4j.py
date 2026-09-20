"""
companies를 원천으로 상장사만 Neo4j Company 노드로 시드한다.
"""

from __future__ import annotations

from collections import defaultdict
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
UPSERT_COMPANIES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Company {name: row.name})
SET c.company_id = row.company_id,
    c.ticker = row.ticker,
    c.corp_code = row.corp_code,
    c.is_listed = row.is_listed,
    c.country = row.country,
    c.market = row.market,
    c.krx100 = row.krx100,
    c.krx300 = row.krx300,
    c.kosdaq150 = row.kosdaq150
"""

MARKET_LABELS = ("KOSPI", "KOSDAQ")


def build_upsert_cypher(market: str | None) -> str:
    """해당 시장 라벨을 리터럴로 박은 upsert Cypher를 만든다.

    부여하지 않는 시장 라벨은 항상 REMOVE 한다. 이전상장(KOSDAQ→KOSPI)이나 원천에
    짝이 없어진 경우 옛 라벨이 남으면 한 노드가 (:KOSPI)와 (:KOSDAQ)를 동시에
    만족해 시장별 스캔이 중복 결과를 낸다.
    """

    stale = [label for label in MARKET_LABELS if label != market]

    clauses = []
    if market:
        clauses.append(f"SET c:{market}")
    clauses.append("REMOVE c" + "".join(f":{label}" for label in stale))

    return UPSERT_COMPANIES_CYPHER + "\n".join(clauses) + "\n"


# US 시드(seed_graph_us_companies, companies_load_us DAG)는 이 잡의 원천이 아니다. 라벨로
# 빼 두지 않으면 AAPL 같은 ticker가 $tickers(국내 종목코드)에 없다는 이유로 전부 지워진다.
FOREIGN_LABELS = ("NYSE", "NASDAQ")

_NOT_FOREIGN = " ".join(f"AND NOT c:{label}" for label in FOREIGN_LABELS)

DELETE_DELISTED_CYPHER = f"""
MATCH (c:Company)
WHERE c.ticker IS NOT NULL {_NOT_FOREIGN}
  AND NOT c.ticker IN $tickers
DETACH DELETE c
"""

COUNT_SEEDED_CYPHER = (
    f"MATCH (c:Company) WHERE c.ticker IS NOT NULL {_NOT_FOREIGN} RETURN count(c) AS seeded"
)


async def seed_graph_companies(rows: list[dict[str, Any]]) -> int:
    """상장 법인 (company_id, name, ticker, corp_code, is_listed, country, market,
    krx100, krx300, kosdaq150)를 Neo4j Company 노드에 upsert하고 시드 수를 반환한다.

    is_listed=false(비상장·상장폐지) 행은 여기서 걸러 넣지 않는다. 사명 변경은 같은
    ticker 노드의 name을 바꿔 이력을 승계하고, 상장폐지는 노드를 삭제한다.
    """

    listed = [row for row in rows if row["is_listed"] and row["ticker"]]

    if len(listed) < MIN_ROWS_SAFETY:
        logger.error("시드 원천(companies)에 상장 법인이 없어 중단합니다(전량 삭제 방지).")
        return 0

    tickers = [row["ticker"] for row in listed]

    # 라벨을 리터럴로 박아야 해서 시장별로 나눠 실행한다. 원천 market이 알 수 없는
    # 값이면 None 묶음으로 보내 라벨 없이(속성만) 적재한다.
    by_market: dict[str | None, list[dict[str, Any]]] = defaultdict(list)
    for row in listed:
        market = row["market"] if row["market"] in MARKET_LABELS else None
        by_market[market].append(row)

    await neo4j_database.execute(RENAME_COMPANIES_CYPHER, {"rows": listed})
    for market, group in by_market.items():
        await neo4j_database.execute(build_upsert_cypher(market), {"rows": group})
    await neo4j_database.execute(DELETE_DELISTED_CYPHER, {"tickers": tickers})

    records = await neo4j_database.execute(COUNT_SEEDED_CYPHER)
    seeded = records[0]["seeded"] if records else 0

    logger.info(
        "Neo4j 상장사 시드 완료: 대상 %d개, 그래프 내 ticker 보유 %d개", len(listed), seeded
    )

    return seeded


# ── US(NYSE·NASDAQ) ───────────────────────────────────────────────────────────
# KR 시드와 같은 순서(사명 변경 → upsert)지만 삭제 단계가 없다. 지수 이탈 종목은 그대로 둔다.
# 예전에는 migrations/neo4j/0002_us_companies.cypher가 이 노드를 만들었다. 키(name=한글명)와
# ticker가 같아 기존 노드를 그대로 흡수한다.
US_MARKET_LABELS = ("NYSE", "NASDAQ")

UPSERT_US_COMPANIES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Company {name: row.name})
SET c.company_id = row.company_id,
    c.ticker = row.ticker,
    c.en_name = row.en_name,
    c.is_listed = true,
    c.country = 'US',
    c.market = row.market,
    c.sp500 = row.sp500
"""

COUNT_US_SEEDED_CYPHER = "MATCH (c:Company) WHERE c:NYSE OR c:NASDAQ RETURN count(c) AS seeded"


def build_us_upsert_cypher(market: str) -> str:
    """시장 라벨을 리터럴로 박은 US upsert Cypher. 다른 US 라벨은 REMOVE 한다.

    이전상장(NYSE↔NASDAQ) 시 옛 라벨이 남으면 시장별 스캔이 중복된다. 0002 시드가 남긴
    kr_name 속성도 함께 지운다.
    """

    if market not in US_MARKET_LABELS:
        raise ValueError(f"US 시장 라벨이 아닙니다: {market}")

    stale = "".join(f":{label}" for label in US_MARKET_LABELS if label != market)
    return UPSERT_US_COMPANIES_CYPHER + f"SET c:{market}\nREMOVE c{stale}, c.kr_name\n"


async def seed_graph_us_companies(rows: list[dict[str, Any]], sp500_tickers: set[str]) -> int:
    """US 법인 (company_id, name, ticker, market, en_name)을 Company 노드에 upsert.

    sp500은 Postgres에 없는 Neo4j 전용 속성이다. 매 회차 us.json에서 다시 계산한
    `sp500_tickers`로 여기서 붙인다 — S&P 500에서 빠진 티커는 다음 회차에 자동으로
    false로 돌아간다. 호출자의 dict를 바꾸지 않도록 사본에 값을 얹는다.
    """

    if not rows:
        logger.error("시드 원천(companies, country='US')이 비어 있어 중단합니다.")
        return 0

    rows = [{**row, "sp500": row["ticker"] in sp500_tickers} for row in rows]

    by_market: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["market"] not in US_MARKET_LABELS:
            logger.warning("알 수 없는 US 시장, 건너뜀: %s %s", row["ticker"], row["market"])
            continue
        by_market[row["market"]].append(row)

    await neo4j_database.execute(RENAME_COMPANIES_CYPHER, {"rows": rows})
    for market, group in by_market.items():
        await neo4j_database.execute(build_us_upsert_cypher(market), {"rows": group})

    records = await neo4j_database.execute(COUNT_US_SEEDED_CYPHER)
    seeded = records[0]["seeded"] if records else 0
    logger.info("Neo4j US 상장사 시드 완료: 대상 %d개, 그래프 내 %d개", len(rows), seeded)
    return seeded
