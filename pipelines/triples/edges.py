"""Triplet → 엣지/원장 행 변환 규칙.

Neo4j 적재와 relation_sources 행 생성이 **같은 변환 규칙**을 써야 두 저장소가 어긋나지
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


def source_row_of(triplet: Triplet, name_to_ticker: dict[str, str]) -> dict | None:
    """Triplet을 relation_sources(근거 원장) 뉴스 행으로 변환한다.

    엔드포인트 식별은 정규명이 기본이고 code(ticker)는 보조 키다 — 매핑에 없는
    미상장/미시드 기업은 code가 NULL로 남았다가 상장 시 백필된다. 간선 자연키
    (subject_name, relation, object_name)는 edge_of와 같은 규칙에서 나오므로
    Neo4j 간선과 1:1로 대응한다.
    """

    edge = edge_of(triplet)
    if edge is None:
        return None
    subject_name, relation, object_name = edge
    # 타입은 원장 스키마의 확장 대비 컬럼 — 현재 파이프라인 엔티티는 기업뿐이라
    # 상수로 채우고, 새 엔티티 타입이 생기면 여기서 분기한다.
    return {
        "subject_name": subject_name,
        "subject_type": "COMPANY",
        "subject_code": name_to_ticker.get(subject_name),
        "relation": relation,
        "object_name": object_name,
        "object_type": "COMPANY",
        "object_code": name_to_ticker.get(object_name),
        "evidence": triplet.evidence,
        "source_sentence": triplet.source_sentence,
        "polarity": triplet.polarity,
        "tense": triplet.tense,
        "item": triplet.item,
    }
