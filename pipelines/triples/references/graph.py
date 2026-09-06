"""Neo4j 참조 데이터 조회.

relation_sources 행을 만들 때 COMPANY 엔드포인트의 ticker(code)를 그래프에서 채워야 한다.
파이프라인 입력이 아니라 변환에 쓰는 **참조**라 `references/`에 둔다 — `loaders/`에
두면 transformer가 loader를 임포트해 E→T→L 방향이 깨진다. 쓰기는 `loaders/neo4j.py`다.
"""

from __future__ import annotations

from pipelines.common.clients.neo4j import neo4j_database


async def fetch_company_tickers(names: list[str]) -> dict[str, str]:
    """상장 기업명(KRX 또는 0002 이후의 미국) 목록에 대한 ticker를 Neo4j에서 한 번에 조회한다.

    트리플 추출로 만들어지는 Company 노드는 name만 가지지만, 정규화 단계에서 표면형을
    사전 정식명(한글 name)으로 재작성하므로 시드된 (:Company {name, ticker, is_listed})
    노드와 name이 일치한다 — 0002 이후로는 미국 기업도 한글 name, is_listed=true로 시드돼
    같은 경로를 타므로, relation_sources.subject_code에 AAPL 같은 미국 ticker가 담길 수
    있다. 상장사가 아니면 매핑에 담기지 않아 code는 NULL로 남는다.
    """

    if not names:
        return {}

    records = await neo4j_database.execute(
        """
        UNWIND $names AS name
        MATCH (c:Company {name: name})
        WHERE c.is_listed
        RETURN c.name AS name, c.ticker AS ticker
        """,
        {"names": names},
    )
    return {record["name"]: record["ticker"] for record in records}
