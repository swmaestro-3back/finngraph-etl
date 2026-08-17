"""삼중항 → news_relations 테이블 행 변환."""

from __future__ import annotations

from pipelines.triplets.edges import binary_edges
from pipelines.triplets.graph.models import Entity, Triplet
from pipelines.triplets.references.graph import fetch_company_tickers


async def build_news_relation_rows(triplets: list[Triplet]) -> list[dict]:
    """삼중항을 news_relations 테이블 행(dict) 목록으로 변환한다.

    1. Neo4j 적재와 동일하게 binary_edges로 이항 엣지 분해 (item 술어는 2개로 분해)
    2. 동일 (subject_name, relation, object_name) 엣지는 배치 내에서 중복 제거
       (news_relations의 UNIQUE 제약과 동일한 키)
    3. COMPANY 엔티티는 Neo4j에서 ticker를 조회해 code로 채운다. 그 외 타입(COUNTRY ISO 등)은
       현재 NULL로 두고 이후 백필한다.
    """

    edges: list[tuple[Entity, str, Entity]] = []
    seen: set[tuple[str, str, str]] = set()
    for triplet in triplets:
        for subject, rel, obj in binary_edges(triplet):
            edge_key = (subject.text, rel, obj.text)
            if edge_key in seen:
                continue
            seen.add(edge_key)
            edges.append((subject, rel, obj))

    # COMPANY 엔드포인트 이름을 모아 한 번의 쿼리로 ticker 조회
    company_names = sorted(
        {
            endpoint.text
            for subject, _, obj in edges
            for endpoint in (subject, obj)
            if endpoint.label == "COMPANY"
        }
    )
    name_to_ticker = await fetch_company_tickers(company_names)

    def _code(entity: Entity) -> str | None:
        return name_to_ticker.get(entity.text) if entity.label == "COMPANY" else None

    return [
        {
            "subject_name": subject.text,
            "subject_type": subject.label,
            "subject_code": _code(subject),
            "relation": rel,
            "object_name": obj.text,
            "object_type": obj.label,
            "object_code": _code(obj),
        }
        for subject, rel, obj in edges
    ]
