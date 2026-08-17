"""Triple → 이항 엣지 분해 규칙.

Neo4j 적재와 news_relations 행 생성이 **같은 분해 규칙**을 써야 두 저장소가 어긋나지
않는다. 그래서 저장소 계층이 아니라 도메인 어휘로 여기 둔다(models 옆).
"""

from __future__ import annotations

from typing import get_args

from pipelines.triples.models import Entity, EntityLabel, Triple
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT

# NER 태그를 Neo4j Label 태그로 변환
# (COMPANY → Company, COUNTRY → Country)
TYPE_TO_LABEL: dict[EntityLabel, str] = {
    label: "".join(part.capitalize() for part in label.split("_"))
    for label in get_args(EntityLabel)
}

# 하나의 트리플관계를 두 개의 트리플관계로 분해할때 사용
_ITEM_DECOMPOSITION: dict[str, tuple[str, str]] = {
    "SUPPLIES_TO": ("SUPPLIES", "SUPPLIED_TO"),
    "EXPORTS_TO": ("EXPORTS", "EXPORTED_TO"),
}


def binary_edges(triple: Triple) -> list[tuple[Entity, str, Entity]]:
    """하나의 Triple을 (subject, rel, object) 이항 엣지 목록으로 분해한다.

    item이 없으면 (subject)-[predicate]->(object) 단일 엣지로,
    item이 있으면 _ITEM_DECOMPOSITION 규칙에 따라 (subject-item), (item-object)
    두 개의 이항 엣지로 분해한다. 분해된 엣지도 각각 하나의 (subject, rel, object)
    트리플로 취급하므로, item은 첫 엣지의 object이자 둘째 엣지의 subject가 된다.

    각 엔드포인트는 Entity(text/label) 그대로 보존하므로, 호출부에서 Neo4j Label로
    매핑하거나(edge_specs) EntityLabel을 그대로 사용할 수 있다(news_relations 적재).
    """

    if triple.item is None:
        # predicate는 FPDF가 PREDICATE_DICT에 등록된 술어에만 트리플을 만들어주므로 항상
        # 화이트리스트 안이지만, 관계 타입으로 직접 삽입되는 값이라 한 번 더 방어적으로 검증한다.
        if triple.predicate not in PREDICATE_DICT:
            return []
        return [(triple.subject, triple.predicate, triple.object)]

    decomposition = _ITEM_DECOMPOSITION.get(triple.predicate)
    # item이 있으나 분해 규칙이 없는 술어면 item을 버리고 이항 관계로만 저장한다.
    if decomposition is None:
        if triple.predicate not in PREDICATE_DICT:
            return []
        return [(triple.subject, triple.predicate, triple.object)]

    rel_subject_item, rel_item_object = decomposition
    return [
        (triple.subject, rel_subject_item, triple.item),
        (triple.item, rel_item_object, triple.object),
    ]


def edge_specs(triple: Triple) -> list[tuple[str, str, str, str, str]]:
    """하나의 Triple을 (subject_label, subject_name, rel, object_label, object_name)
    Neo4j 엣지 목록으로 변환한다. NER Label을 Neo4j Label로 매핑한다.
    """

    return [
        (TYPE_TO_LABEL[subject.label], subject.text, rel, TYPE_TO_LABEL[obj.label], obj.text)
        for subject, rel, obj in binary_edges(triple)
    ]
