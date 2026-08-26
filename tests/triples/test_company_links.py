"""뉴스-기업 연결 transformer 단위 테스트 (Neo4j/DB는 monkeypatch로 대체)."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip(
    "pipelines.triples.ontology.predicate_dict",
    reason="비공개 ontology가 없는 체크아웃(CI)에서는 스킵",
)

from pipelines.triples.models import Entity, Triplet  # noqa: E402


def _triplet(subject: str, obj: str, predicate: str = "SUPPLIES_TO") -> Triplet:
    return Triplet(
        subject=Entity(text=subject),
        predicate=predicate,
        object=Entity(text=obj),
        source_sentence="원문 문장",
        evidence="근거 문장",
        polarity="affirmed",
        tense="past_or_present_fact",
    )


def test_collect_company_names_dedupes_endpoints():
    from pipelines.triples.transformers.company_links import collect_company_names

    triplets = [
        _triplet("삼성전자", "테슬라"),
        _triplet("삼성전자", "LG에너지솔루션"),
    ]

    assert collect_company_names(triplets) == ["LG에너지솔루션", "삼성전자", "테슬라"]


def test_collect_company_names_skips_unregistered_predicate():
    from pipelines.triples.transformers.company_links import collect_company_names

    assert collect_company_names([_triplet("A", "B", predicate="미등록술어")]) == []


def test_resolve_company_ids_maps_via_ticker(monkeypatch):
    from pipelines.triples.transformers import company_links

    async def fake_fetch_tickers(names):
        # 테슬라는 비상장(그래프 매핑 없음) 시나리오
        return {"삼성전자": "005930"}

    monkeypatch.setattr(company_links, "fetch_company_tickers", fake_fetch_tickers)
    monkeypatch.setattr(
        company_links, "fetch_company_ids_by_tickers", lambda tickers: {"005930": 42}
    )

    ids = asyncio.run(company_links.resolve_company_ids([_triplet("삼성전자", "테슬라")]))

    assert ids == [42]
