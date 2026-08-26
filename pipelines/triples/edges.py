"""Triplet → Neo4j 엣지 변환 규칙.

Neo4j 적재와 relation_source 행 생성이 **같은 변환 규칙**을 써야 두 저장소가 어긋나지
않는다. 그래서 저장소 계층이 아니라 도메인 어휘로 여기 둔다(models 옆).

구 파이프라인과 달리 엔티티가 기업(COMPANY)뿐이라 라벨 매핑이 없고, item이 자유
텍스트라 SUPPLIES_TO 2-엣지 분해도 없다. Triplet 하나가 곧 (subject, predicate,
object) 엣지 하나이며, item은 엣지 메타데이터로만 전달된다.
"""

from __future__ import annotations

from pipelines.triples.models import Triplet
from pipelines.triples.ontology.predicate_dict import PREDICATE_DICT

# 모든 엔드포인트는 gazetteer로 정규화된 기업명이다.
NODE_LABEL = "Company"


def edge_of(triplet: Triplet) -> tuple[str, str, str] | None:
    """Triplet을 (subject_name, predicate, object_name) 엣지로 변환한다.

    predicate는 TripletBuilder가 PREDICATE_DICT에 등록된 술어만 통과시키므로 항상
    화이트리스트 안이지만, 관계 타입으로 직접 삽입되는 값이라 한 번 더 방어적으로
    검증한다. 미등록 술어면 None을 반환한다.
    """

    if triplet.predicate not in PREDICATE_DICT:
        return None
    return (triplet.subject.text, triplet.predicate, triplet.object.text)
