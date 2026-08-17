"""data/seed의 country.json, krx.json, us.json을 읽어 Neo4j에 적재한다.

seed()를 진입점으로 노출한다. DB에 노드가 하나라도 있으면 스킵한다.
드라이버 수명은 관리하지 않으므로 호출자가 `async with neo4j_database:` 안에서 불러야 한다.
"""

import json
from pathlib import Path

from pipelines.common.clients.neo4j import neo4j_database

SEED_DIR = Path(__file__).resolve().parent / "data" / "seed"


def _load(filename: str) -> list[dict]:
    with open(SEED_DIR / filename, encoding="utf-8") as f:
        return json.load(f)


# UNIQUE CONSTRAINT 생성
# Neo4j에는 PK라는 개념이 없고 이런 속성이 자동으로 부여되지 않는다.
# ENTITY 나 RELATION의 ID는 Neo4j가 자동으로 부여하는 고유한 값으로 내 맘대로 지정할 수 없다.
# 따라서 PK 처럼 동작할 필드를 지정해주어야 한다.
# 이를 UNIQUE CONSTRAINTS를 통해 구현한다.
# UNIQUE CONSTRAINTS를 걸면 자동으로 B-TREE를 생성해준다.
async def create_constraints() -> None:
    # COMPANY TICKER
    await neo4j_database.execute(
        """
        CREATE CONSTRAINT company_ticker_unique IF NOT EXISTS
        FOR (c:Company) REQUIRE c.ticker IS UNIQUE
        """
    )

    # COUNTRY ISO_NUM
    await neo4j_database.execute(
        """
        CREATE CONSTRAINT country_iso_num_unique IF NOT EXISTS
        FOR (c:Country) REQUIRE c.iso_num IS UNIQUE
        """
    )
    print("Constraints created")


async def seed_countries() -> None:
    rows = [
        {
            "name": row["country_nm"],
            "iso_alp2": row["country_iso_alp2"],
            "iso_num": row["iso_num"],
        }
        for row in _load("country.json")
        if row["iso_num"].strip()
    ]

    await neo4j_database.execute(
        """
        UNWIND $rows AS row
        MERGE (c:Country {iso_num: row.iso_num})
        SET c.name = row.name, c.iso_alp2 = row.iso_alp2
        """,
        {"rows": rows},
    )
    print(f"Country: {len(rows)} nodes merged")


async def seed_krx_companies() -> None:
    # KOSDAQ GLOBAL은 KOSDAQ 라벨로 합쳐서 저장한다.
    rows_by_label: dict[str, list[dict]] = {"KOSPI": [], "KOSDAQ": []}
    for row in _load("krx.json"):
        label = "KOSPI" if row["market"] == "KOSPI" else "KOSDAQ"
        rows_by_label[label].append({"ticker": row["ticker"], "name": row["name"]})

    for label, rows in rows_by_label.items():
        await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (c:Company:{label} {{ticker: row.ticker}})
            SET c.name = row.name
            """,
            {"rows": rows},
        )
        print(f"Company:{label}: {len(rows)} nodes merged")


async def seed_us_companies() -> None:
    rows_by_label: dict[str, list[dict]] = {"NYSE": [], "NASDAQ": []}
    for row in _load("us.json"):
        rows_by_label[row["market"]].append(
            {
                "ticker": row["ticker"],
                "name": row["name"],
                "kr_name": row["kr_name"],
                "sp500": row["sp500"] == "true",
            }
        )

    for label, rows in rows_by_label.items():
        await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (c:Company:{label} {{ticker: row.ticker}})
            SET c.name = row.name, c.kr_name = row.kr_name, c.sp500 = row.sp500
            """,
            {"rows": rows},
        )
        print(f"Company:{label}: {len(rows)} nodes merged")


async def seed():
    # seed가 이미 존재한다면 스킵
    records = await neo4j_database.execute("MATCH (n) RETURN count(n) AS c")
    node_count = records[0]["c"]
    if node_count > 0:
        print(f"Skip seeding: {node_count} nodes already exist")
        return

    # 실제 SEED DATA가 들어오기 전에 UNIQUE CONSTRAINTS를 설정해줄 수 있다.
    await create_constraints()

    await seed_countries()
    await seed_krx_companies()
    await seed_us_companies()
