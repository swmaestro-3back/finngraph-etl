"""promote_events 헬퍼 — 스텁 titler/loader 로 실패 격리와 집계."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from flashtext import KeywordProcessor

from pipelines.events.jobs import promote_events as job
from pipelines.events.models import ClusterCandidate, EventDraft, MemberArticle

T0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)  # KST 9/1 09:00


class FakeExtractor:
    def __init__(self):
        self._processor = KeywordProcessor(case_sensitive=True)
        self._processor.add_keywords_from_dict({"삼성전자": ["삼성전자", "삼전"], "기아": ["기아"]})

    def canonicalize(self, text: str) -> str:
        return self._processor.replace_keywords(text)

    def extract(self, text: str) -> list:
        return [SimpleNamespace(text=n) for n in self._processor.extract_keywords(text)]


class StubTitler:
    def __init__(self, draft: EventDraft | Exception):
        self._draft = draft
        self.calls = 0

    async def draft(self, dated_texts, candidates) -> EventDraft:
        self.calls += 1
        if isinstance(self._draft, Exception):
            raise self._draft
        return self._draft


def _cluster(cluster_id: int = 1) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id,
        representative_news_id=11,
        keywords=["삼성전자"],
        original_size=3,
        member_count=2,
        first_published_at=T0,
        last_published_at=T0 + timedelta(hours=1),
    )


def _members() -> list[MemberArticle]:
    return [
        MemberArticle(
            news_id=11,
            title="삼전 유상증자 결정",
            summary="삼전이 결의했다.",
            text="x",
            published_at=T0,
        ),
        MemberArticle(
            news_id=12,
            title="삼성전자 주가 급락",
            summary=None,
            text="유증 소식에 급락. 기아 언급",
            published_at=None,
        ),
    ]


def test_build_create_input_dates_and_candidates():
    dated_texts, candidates = job.build_create_input(_members(), FakeExtractor(), lead_chars=600)

    assert dated_texts == [
        (date(2026, 9, 1), "삼성전자 유상증자 결정\n삼성전자이 결의했다."),
        (None, "삼성전자 주가 급락\n유증 소식에 급락. 기아 언급"),
    ]
    assert candidates == ["삼성전자", "기아"]


def test_create_one_success_records_edges(monkeypatch):
    captured = {}

    async def fake_create_event(record):
        captured["record"] = record
        return 1

    monkeypatch.setattr(job, "create_event", fake_create_event)
    titler = StubTitler(
        EventDraft(companies=["삼성전자", "엔비디아"], title="[속보] 삼성전자 유상증자")
    )

    outcome = asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            ["삼성전자", "기아"],
            titler,
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )

    assert outcome == "created"
    record = captured["record"]
    assert record.cluster_id == 1
    assert record.title == "삼성전자 유상증자"  # 검증이 태그를 벗겼다
    assert record.companies == ["삼성전자"]  # 후보 밖 엔비디아 제거
    assert record.news_ids == [11, 12]
    assert record.keywords == ["삼성전자"]


def test_create_one_without_edges(monkeypatch):
    async def fake_create_event(record):
        return 0

    monkeypatch.setattr(job, "create_event", fake_create_event)

    outcome = asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            ["삼성전자"],
            StubTitler(EventDraft(companies=[], title="삼성전자 유상증자")),
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )

    assert outcome == "created_without_edges"


@pytest.mark.parametrize(
    "titler",
    [
        StubTitler(RuntimeError("bedrock down")),
        StubTitler(EventDraft(companies=[], title="")),  # 검증 실패
        StubTitler(EventDraft(companies=[], title="가" * 61)),  # 길이 초과
    ],
)
def test_create_one_failure_is_isolated(monkeypatch, titler):
    called = {"n": 0}

    async def fake_create_event(record):
        called["n"] += 1
        return 1

    monkeypatch.setattr(job, "create_event", fake_create_event)

    outcome = asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            ["삼성전자"],
            titler,
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )

    assert outcome == "failed"
    assert called["n"] == 0


def test_create_one_loader_failure(monkeypatch):
    async def fake_create_event(record):
        raise RuntimeError("neo4j down")

    monkeypatch.setattr(job, "create_event", fake_create_event)

    outcome = asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            ["삼성전자"],
            StubTitler(EventDraft(companies=["삼성전자"], title="삼성전자 유상증자")),
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )

    assert outcome == "failed"


def test_summarize_stats_invariant_holds():
    stats = {
        "scanned": 10,
        "created": 3,
        "created_without_edges": 1,
        "refreshed": 4,
        "refresh_failed": 0,
        "skipped_no_candidates": 1,
        "skipped_over_limit": 1,
        "failed": 1,
    }

    assert job.summarize_stats(dict(stats)) == stats


def test_summarize_stats_invariant_violation_raises():
    stats = {
        "scanned": 10,
        "created": 3,
        "created_without_edges": 1,
        "refreshed": 4,
        "refresh_failed": 0,
        "skipped_no_candidates": 0,
        "skipped_over_limit": 0,
        "failed": 0,
    }

    with pytest.raises(ValueError):
        job.summarize_stats(stats)


def test_select_for_llm_applies_limit_after_candidate_filter():
    prepared = [
        (_cluster(1), _members(), [(None, "t\nb")], ["삼성전자"]),
        (_cluster(2), _members(), [(None, "t\nb")], []),  # 후보 0 → 제외
        (_cluster(3), _members(), [(None, "t\nb")], ["기아"]),
        (_cluster(4), _members(), [(None, "t\nb")], ["삼성전자"]),
    ]

    eligible, skipped_no_candidates, skipped_over_limit = job.select_for_llm(prepared, limit=2)

    assert [item[0].cluster_id for item in eligible] == [1, 3]
    assert skipped_no_candidates == 1
    assert skipped_over_limit == 1
