"""뉴스-기업 연결 transformer 단위 테스트."""

from __future__ import annotations

import pytest

pytest.importorskip(
    "pipelines.triples.ontology.predicate_dict",
    reason="비공개 ontology가 없는 체크아웃(CI)에서는 스킵",
)

from pipelines.triples.models import Entity, Triplet  # noqa: E402


def _entity(name: str, company_id: int) -> Entity:
    return Entity(
        text=name, canonical=name, company_id=company_id, stock_id=company_id, ticker=name
    )


def _triplet(subject: Entity, obj: Entity, predicate: str = "SUPPLIES_TO") -> Triplet:
    return Triplet(
        subject=subject,
        predicate=predicate,
        object=obj,
        source_sentence="원문 문장",
        evidence="근거 문장",
        polarity="affirmed",
        tense="past_or_present_fact",
        subject_impact="positive",
        object_impact="neutral",
    )


def test_collect_company_ids_dedupes_endpoints():
    from pipelines.triples.transformers.company_links import collect_company_ids

    samsung, tesla, lges = _entity("삼성전자", 3), _entity("테슬라", 1), _entity("LG엔솔", 2)
    triplets = [_triplet(samsung, tesla), _triplet(samsung, lges)]

    assert collect_company_ids(triplets) == [1, 2, 3]


def test_collect_company_ids_skips_unregistered_predicate():
    from pipelines.triples.transformers.company_links import collect_company_ids

    triplet = _triplet(_entity("A", 1), _entity("B", 2), predicate="미등록술어")

    assert collect_company_ids([triplet]) == []
