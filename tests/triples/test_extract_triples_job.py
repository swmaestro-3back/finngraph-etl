"""extract_triples job 단위 테스트. 그래프·DB 는 갈아끼운다."""

from __future__ import annotations

import asyncio
from datetime import date

import pytest

pytest.importorskip(
    "pipelines.triples.ontology.predicate_dict",
    reason="비공개 ontology가 없는 체크아웃(CI)에서는 스킵",
)

from pipelines.triples.jobs import extract_triples as job  # noqa: E402
from pipelines.triples.models import Entity, Triplet  # noqa: E402


def _entity(name: str, company_id: int) -> Entity:
    return Entity(
        text=name, canonical=name, company_id=company_id, stock_id=company_id, ticker=name
    )


def _triplet() -> Triplet:
    return Triplet(
        subject=_entity("엘앤에프", 1),
        predicate="SUPPLIES_TO",
        object=_entity("삼성SDI", 2),
        source_sentence="원문 문장",
        evidence="근거 문장",
        polarity="affirmed",
        tense="past_or_present_fact",
        subject_impact="positive",
        object_impact="neutral",
    )


class _Runner:
    def __init__(self):
        self.calls: list[tuple] = []

    async def ainvoke(self, news_id, article, entities):
        self.calls.append((news_id, article, [entity.text for entity in entities]))
        return {"triplets": [_triplet()]}


def _patch_io(monkeypatch, entities):
    calls: dict[str, list] = {"ledger": [], "marked": [], "linked": []}

    def fake_ledger(news_id, mentioned_at, rows):
        calls["ledger"].append((news_id, len(rows)))

    async def fake_sync(summaries):
        return None

    def fake_linked(text, company_ids):
        calls["linked"].append((text, list(company_ids)))
        return entities

    monkeypatch.setattr(job, "insert_relation_sources", fake_ledger)
    monkeypatch.setattr(job, "fetch_edge_summaries", lambda keys: [])
    monkeypatch.setattr(job, "sync_edge_summaries", fake_sync)
    monkeypatch.setattr(job, "linked_entities", fake_linked)
    monkeypatch.setattr(
        job,
        "mark_triple_extraction_result",
        lambda has, none: calls["marked"].append((has, none)),
    )
    return calls


ITEM = {"news_id": 7, "text": "본문", "mentioned_at": date(2026, 10, 2), "company_ids": [1, 2]}


def test_process_item_passes_linked_entities_and_writes_ledger_and_graph(monkeypatch):
    calls = _patch_io(monkeypatch, [_entity("엘앤에프", 1), _entity("삼성SDI", 2)])
    runner = _Runner()

    status = asyncio.run(job._process_item(runner, ITEM))

    assert status == "has_triples"
    # 엔티티는 news_companies 의 기업으로 만든다. 그래프는 그 엔티티로 관계 추출부터 돈다.
    assert calls["linked"] == [("본문", [1, 2])]
    assert runner.calls == [("7", "본문", ["엘앤에프", "삼성SDI"])]
    assert calls["ledger"] == [(7, 1)]
    assert calls["marked"] == [([7], [])]
    # news_companies 는 수집 단계(news/repositories/news_companies.py)만 쓴다
    assert not hasattr(job, "insert_news_companies")


def test_process_item_marks_no_triples_without_two_entities(monkeypatch):
    calls = _patch_io(monkeypatch, [_entity("엘앤에프", 1)])
    runner = _Runner()

    status = asyncio.run(job._process_item(runner, ITEM))

    # 관계에는 엔티티가 둘 필요하다 — LLM 을 부르지 않고 "시도했으나 없음"으로 마킹한다
    assert status == "no_triples"
    assert runner.calls == []
    assert calls["ledger"] == []
    assert calls["marked"] == [([], [7])]


def test_process_item_counts_companies_not_spellings(monkeypatch):
    # 연결 기업은 하나인데 본문에 두 표기("LG에너지솔루션(LG엔솔)")로 나온다. 엔티티는 둘이지만
    # 관계의 양 끝이 될 기업이 하나뿐이라 LLM 을 부르지 않는다.
    calls = _patch_io(monkeypatch, [_entity("LG에너지솔루션", 1), _entity("LG엔솔", 1)])
    runner = _Runner()

    status = asyncio.run(job._process_item(runner, ITEM))

    assert status == "no_triples"
    assert runner.calls == []
    assert calls["marked"] == [([], [7])]


def test_process_item_leaves_article_unprocessed_when_graph_fails(monkeypatch):
    calls = _patch_io(monkeypatch, [_entity("엘앤에프", 1), _entity("삼성SDI", 2)])

    class _Failing:
        async def ainvoke(self, news_id, article, entities):
            raise RuntimeError("bedrock down")

    assert asyncio.run(job._process_item(_Failing(), ITEM)) == "failed"
    # 마킹하지 않아 triple_extracted 가 NULL 로 남고 다음 런이 다시 시도한다
    assert calls["marked"] == []
