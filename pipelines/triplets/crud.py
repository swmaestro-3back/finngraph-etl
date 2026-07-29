# Neo4j CRUD
from __future__ import annotations

from collections import defaultdict

from pipelines.common.neo4j import neo4j_database
from pipelines.triplets.graph.models import Triplet
from pipelines.triplets.graph.ontology.predicate_dict import PREDICATE_DICT

# 개체명 타입 → Neo4j 노드 라벨
_TYPE_TO_LABEL: dict[str, str] = {
    "COMPANY": "Company",
    "COUNTRY": "Country",
    "COMMODITY": "Commodity",
    "PRODUCT": "Product",
}

# item(3-argument)을 가진 술어를 두 개의 이항 관계로 분해할 때 쓰는 관계 타입.
# (subject)-[앞]->(item), (item)-[뒤]->(object) 순서다. "공급=생산"처럼 원문에 없는 사실을
# 만들지 않도록, 원래 술어의 의미를 그대로 유지하는 중립적인 이름만 사용한다.
_ITEM_DECOMPOSITION: dict[str, tuple[str, str]] = {
    "SUPPLIES_TO": ("SUPPLIES", "SUPPLIED_TO"),
    "EXPORTS_TO": ("EXPORTS", "EXPORTED_TO"),
}


def _edge_specs(triple: Triplet) -> list[tuple[str, str, str, str, str]]:
    """하나의 Triplet을 (from_label, from_name, rel, to_label, to_name) 엣지 목록으로 변환한다.

    item이 없으면 (subject)-[predicate]->(object) 단일 엣지로,
    item이 있으면 _ITEM_DECOMPOSITION 규칙에 따라 (subject-item), (item-object)
    두 개의 이항 엣지로 분해한다.
    """
    subject_label = _TYPE_TO_LABEL.get(triple.subject_type)
    object_label = _TYPE_TO_LABEL.get(triple.object_type)
    # 타입 매핑에 없는 개체명은 저장하지 않는다 (알 수 없는 라벨의 Cypher 삽입 방지).
    if subject_label is None or object_label is None:
        return []

    if triple.item is None:
        # predicate는 FPDF가 PREDICATE_DICT에 등록된 술어에만 트리플을 만들어주므로 항상
        # 화이트리스트 안이지만, 관계 타입으로 직접 삽입되는 값이라 한 번 더 방어적으로 검증한다.
        if triple.predicate not in PREDICATE_DICT:
            return []
        return [(subject_label, triple.subject, triple.predicate, object_label, triple.object)]

    item_label = _TYPE_TO_LABEL.get(triple.item_type or "")
    decomposition = _ITEM_DECOMPOSITION.get(triple.predicate)
    # item이 있으나 타입/분해 규칙이 없으면 item을 버리고 이항 관계로만 저장한다.
    if item_label is None or decomposition is None:
        if triple.predicate not in PREDICATE_DICT:
            return []
        return [(subject_label, triple.subject, triple.predicate, object_label, triple.object)]

    rel_subject_item, rel_item_object = decomposition
    return [
        (subject_label, triple.subject, rel_subject_item, item_label, triple.item),
        (item_label, triple.item, rel_item_object, object_label, triple.object),
    ]


async def upsert_triplets(news_id: str, triplets: list[Triplet]) -> None:
    """워크플로우가 추출한 Triplet들을 Neo4j에 반영한다.

    item(품목)을 가진 삼항 술어는 (subject-item), (item-object) 두 이항 관계로 분해하고,
    각 관계에 news_id와 원문 문장(source_sentence)을 실어 저장한다. 이렇게 하면 같은
    뉴스·문장에서 파생된 관계들을 (news_id, source_sentence)로 다시 엮을 수 있다.
    같은 (news_id, source_sentence)의 동일 관계는 MERGE로 한 번만 생성되고, 다른 뉴스의
    동일 관계는 provenance가 달라 평행 엣지로 남는다.

    노드는 name 기준으로 MERGE한다. 시드된 Company/Country 노드는 name이 일치하면 그대로
    매칭되고, 신규 Product/Commodity는 새로 생성된다.
    """
    # (from_label, rel, to_label) 시그니처가 같은 엣지끼리 묶어 UNWIND 한 번으로 MERGE 한다.
    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for triplet in triplets:
        for from_label, from_name, rel, to_label, to_name in _edge_specs(triplet):
            grouped[(from_label, rel, to_label)].append(
                {
                    "from_name": from_name,
                    "to_name": to_name,
                    # None이면 MERGE가 null 프로퍼티로 에러를 내므로 빈 문자열로 정규화한다.
                    "source_sentence": triplet.source_sentence or "",
                }
            )

    for (from_label, rel, to_label), rows in grouped.items():
        # from_label/to_label은 _TYPE_TO_LABEL, rel은 PREDICATE_DICT·_ITEM_DECOMPOSITION에서만
        # 오므로 화이트리스트로 검증된 값이다. Cypher는 라벨·관계 타입의 파라미터 바인딩을
        # 지원하지 않아 seed_db와 동일하게 f-string으로 직접 삽입하고, 값은 파라미터로 넘긴다.
        await neo4j_database.execute(
            f"""
            UNWIND $rows AS row
            MERGE (s:{from_label} {{name: row.from_name}})
            MERGE (o:{to_label} {{name: row.to_name}})
            MERGE (s)-[:{rel} {{news_id: $news_id, source_sentence: row.source_sentence}}]->(o)
            """,
            {"rows": rows, "news_id": news_id},
        )