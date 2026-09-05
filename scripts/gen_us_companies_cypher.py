"""us.json을 Neo4j 시드 마이그레이션(Cypher)으로 변환한다.

neo4j-init은 migrations/neo4j/*.cypher만 실행하므로 JSON을 런타임에 읽을 수단이 없다
(APOC은 dev의 원격 인스턴스에서 보장되지 않는다). 그래서 원천 JSON을 UNWIND 리터럴로
펼친 Cypher를 만들어 두고, 원천이 바뀌면 이 스크립트로 다시 생성한다.

    uv run python scripts/gen_us_companies_cypher.py

노드 키는 KRX 시드(pipelines/companies/loaders/neo4j.py)와 같은 `name`(한글 정규명 =
us.json kr_name)이다. 삼중항 파이프라인은 gazetteer 정규명으로 `MERGE (:Company {name})`
하므로, name 키로 MERGE 해야 뉴스가 먼저 만든 이름만 있는 노드를 흡수해 간선을 보존한다.
neo4j-init 은 매 기동마다 이 파일을 재실행하므로 두 블록 모두 멱등이어야 한다.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "us.json"
TARGET = ROOT / "migrations" / "neo4j" / "0002_us_companies.cypher"

# 라벨은 파라미터로 못 넘겨 리터럴로 박아야 한다 → 시장별로 블록을 나눈다.
# (pipelines/companies/loaders/neo4j.py의 build_upsert_cypher와 같은 이유)
MARKET_LABELS = ("NASDAQ", "NYSE")

HEADER = """\
// us.json에서 생성됨 — 직접 수정하지 말 것.
// 재생성: uv run python scripts/gen_us_companies_cypher.py
"""

BLOCK = """
// {market} {count}개
// [1] 한글명 변경 — ticker 로 찾아 name 만 교체. [2] 보다 먼저 와야 한다(KRX 시드와 같은 순서)
UNWIND [
{rows}
] AS row
MATCH (old:Company {{ticker: row.ticker}})
WHERE old.name <> row.kr_name
SET old.name = row.kr_name;

// [2] name 키 MERGE — 삼중항이 만든 {{name: kr_name, ticker: null}} 노드를 흡수해 간선을
//     보존한다. 없으면 새로 만든다. 두 번째 기동부터는 같은 값을 다시 SET 하는 no-op
UNWIND [
{rows}
] AS row
MERGE (c:Company {{name: row.kr_name}})
SET c:{market},
    c.ticker = row.ticker,
    c.en_name = row.name,
    c.is_listed = true,
    c.market = row.market,
    c.sp500 = row.sp500
REMOVE c{stale}, c.kr_name;
"""


def _literal(row: dict[str, str]) -> str:
    """맵 리터럴 한 줄. 값 이스케이프는 JSON 규칙이 Cypher와 동일해 json.dumps를 쓴다."""

    def quote(value: str) -> str:
        return json.dumps(value, ensure_ascii=False)

    return (
        f"  {{ticker: {quote(row['ticker'])}, name: {quote(row['name'])}, "
        f"kr_name: {quote(row['kr_name'])}, market: {quote(row['market'])}, "
        f"sp500: {'true' if row['sp500'] == 'true' else 'false'}}}"
    )


def main() -> None:
    rows = json.loads(SOURCE.read_text(encoding="utf-8"))

    unknown = {row["market"] for row in rows} - set(MARKET_LABELS)
    if unknown:
        raise SystemExit(f"알 수 없는 market 값: {sorted(unknown)}")

    blocks = []
    for market in MARKET_LABELS:
        group = [row for row in rows if row["market"] == market]
        stale = "".join(f":{label}" for label in MARKET_LABELS if label != market)
        blocks.append(
            BLOCK.format(
                market=market,
                count=len(group),
                rows=",\n".join(_literal(row) for row in group),
                stale=stale,
            )
        )

    TARGET.write_text(HEADER + "".join(blocks), encoding="utf-8")
    print(f"{TARGET.relative_to(ROOT)} 생성: {len(rows)}개")


if __name__ == "__main__":
    main()
