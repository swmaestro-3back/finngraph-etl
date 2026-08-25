"""공급계약 공시의 Neo4j 적재.

트리플 파이프라인과 같은 형식으로 (제출사)-[:SUPPLIES_TO]->(계약상대) 간선을 쓴다 —
SUPPLIES_TO 술어 정의가 "공급계약 체결·수주 포함"이라(ontology/predicate_dict.py) 뉴스
유래 간선과 같은 타입으로 합쳐지고, 같은 두 회사 쌍이면 하나의 간선에 뉴스 provenance
(news_ids)와 공시 provenance(rcept_nos)가 나란히 쌓인다.

노드는 themes 로더처럼 시드된 Company 를 **ticker 로 MATCH** 한다(이름은 표기가 흔들려
MERGE 하면 중복 노드가 생긴다). 시드에 없는 ticker 는 간선이 만들어지지 않는다.

간선 속성은 rcept_nos·links 두 배열뿐이고, disclosures 테이블 전량 조회 결과로 매번
덮어쓴다 — 원천이 Postgres 라 그래프 쪽 증분·중복 관리가 필요 없다.
"""

from __future__ import annotations

from collections import defaultdict

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.disclosures.models import SupplyContractEdge

logger = get_logger(__name__)


async def upsert_supply_edges(edges: list[SupplyContractEdge]) -> None:
    """SUPPLIES_TO 간선을 적재한다.

    같은 (제출사, 계약상대) 쌍의 공시 여러 건은 간선 하나에 rcept_nos/links 배열로
    쌓는다. 한 쿼리 안에서 같은 간선을 여러 row 가 갱신하면 플래너의 Eager 평가 때문에
    서로를 덮어쓰므로, 쌍 단위로 미리 묶어 간선당 row 하나로 만든다.
    """

    if not edges:
        return

    grouped: dict[tuple[str, str], list[SupplyContractEdge]] = defaultdict(list)
    for edge in edges:
        grouped[(edge.filer_ticker, edge.counterparty_ticker)].append(edge)

    rows = [
        {
            "filer_ticker": filer_ticker,
            "counterparty_ticker": counterparty_ticker,
            "rcept_nos": [e.rcept_no for e in pair_edges],
            "links": [e.link for e in pair_edges],
        }
        for (filer_ticker, counterparty_ticker), pair_edges in grouped.items()
    ]

    records = await neo4j_database.execute(
        """
        UNWIND $rows AS row
        MATCH (s:Company {ticker: row.filer_ticker})
        MATCH (o:Company {ticker: row.counterparty_ticker})
        MERGE (s)-[r:SUPPLIES_TO]->(o)
        SET r.rcept_nos = row.rcept_nos,
            r.links = row.links
        RETURN count(r) AS linked
        """,
        {"rows": rows},
    )

    linked = records[0]["linked"] if records else 0
    logger.info(
        "공급계약 간선 적재: 공시 %d건 → 회사쌍 %d개, 간선 반영 %d개 (미반영은 시드에 없는 ticker)",
        len(edges),
        len(rows),
        linked,
    )
