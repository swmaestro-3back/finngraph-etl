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
    async def ainvoke(self, news_id, article):
        return {"triplets": [_triplet()]}


def test_process_item_writes_ledger_and_graph_but_not_news_companies(monkeypatch):
    calls: dict[str, list] = {"ledger": [], "marked": []}

    def fake_ledger(news_id, mentioned_at, rows):
        calls["ledger"].append((news_id, len(rows)))

    async def fake_sync(summaries):
        return None

    monkeypatch.setattr(job, "insert_relation_sources", fake_ledger)
    monkeypatch.setattr(job, "fetch_edge_summaries", lambda keys: [])
    monkeypatch.setattr(job, "sync_edge_summaries", fake_sync)
    monkeypatch.setattr(
        job,
        "mark_triple_extraction_result",
        lambda has, none: calls["marked"].append((has, none)),
    )

    item = {"news_id": 7, "text": "본문", "mentioned_at": date(2026, 10, 2)}
    status = asyncio.run(job._process_item(_Runner(), item))

    assert status == "has_triples"
    assert calls["ledger"] == [(7, 1)]
    assert calls["marked"] == [([7], [])]
    # news_companies 는 수집 단계(news/repositories/news_companies.py)만 쓴다
    assert not hasattr(job, "insert_news_companies")
