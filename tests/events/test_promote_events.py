"""promote_events 헬퍼 — 스텁 generator/loader 로 실패 격리와 집계."""

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


class StubGenerator:
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
    generator = StubGenerator(
        EventDraft(companies=["삼성전자", "엔비디아"], title="[속보] 삼성전자 유상증자")
    )

    outcome = asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            ["삼성전자", "기아"],
            generator,
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
            StubGenerator(EventDraft(companies=[], title="삼성전자 유상증자")),
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )

    assert outcome == "created_without_edges"


@pytest.mark.parametrize(
    "generator",
    [
        StubGenerator(RuntimeError("bedrock down")),
        StubGenerator(EventDraft(companies=[], title="")),  # 검증 실패
        StubGenerator(EventDraft(companies=[], title="가" * 61)),  # 길이 초과
    ],
)
def test_create_one_failure_is_isolated(monkeypatch, generator):
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
            generator,
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
            StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 유상증자")),
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


# ---- _run 통합 (모든 I/O 시임을 스텁으로 대체) --------------------------------


class _StubNeo4jDatabase:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _event_settings(**overrides) -> SimpleNamespace:
    base = dict(
        min_size=2,
        scan_days=15,
        max_items_per_run=2,
        llm_max_concurrency=2,
        lead_chars=600,
        title_max_chars=60,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _no_candidate_members() -> list[MemberArticle]:
    """gazetteer 에 없는 이름만 담은 멤버 — 후보 0 케이스용."""

    return [
        MemberArticle(
            news_id=901,
            title="무관 발표",
            summary="이 사건과는 관련 없는 내용이다.",
            text="x",
            published_at=T0,
        )
    ]


def test_run_mixed_refresh_and_create(monkeypatch):
    """5 클러스터: 기존 2(전량 갱신) + 신규 3(후보0 1, 유효 2 → 간선1/간선0)."""

    promotable = [_cluster(cid) for cid in (1, 2, 3, 4, 5)]
    members_by_cluster = {3: _no_candidate_members(), 4: _members(), 5: _members()}
    edges_by_cluster = {4: 0, 5: 1}

    async def fake_existing(cluster_ids):
        return {1, 2}

    async def fake_refresh(refreshes):
        return len(refreshes)

    async def fake_create_event(record):
        return edges_by_cluster[record.cluster_id]

    monkeypatch.setattr(job, "get_event_settings", lambda: _event_settings())
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "fetch_promotable_clusters", lambda min_size, since: promotable)
    monkeypatch.setattr(job, "fetch_existing_event_ids", fake_existing)
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {i: [i * 100] for i in ids})
    monkeypatch.setattr(job, "fetch_cluster_members", lambda ids: members_by_cluster)
    monkeypatch.setattr(job, "refresh_events", fake_refresh)
    monkeypatch.setattr(job, "create_event", fake_create_event)

    generator = StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 이슈"))
    stats = asyncio.run(job._run(FakeExtractor(), generator))

    assert stats == {
        "scanned": 5,
        "created": 2,
        "created_without_edges": 1,
        "refreshed": 2,
        "refresh_failed": 0,
        "skipped_no_candidates": 1,
        "skipped_over_limit": 0,
        "failed": 0,
    }
    assert job.summarize_stats(stats) == stats


def test_run_refresh_batch_failure_still_creates(monkeypatch):
    """갱신 배치가 통째로 실패해도 refresh_failed 로 흡수하고 생성은 계속된다."""

    promotable = [_cluster(1), _cluster(2), _cluster(3)]
    members_by_cluster = {2: _members(), 3: _members()}

    async def fake_existing(cluster_ids):
        return {1}

    async def fake_refresh_raises(refreshes):
        raise RuntimeError("neo4j 다운")

    async def fake_create_event(record):
        return 1

    monkeypatch.setattr(job, "get_event_settings", lambda: _event_settings())
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "fetch_promotable_clusters", lambda min_size, since: promotable)
    monkeypatch.setattr(job, "fetch_existing_event_ids", fake_existing)
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {i: [i] for i in ids})
    monkeypatch.setattr(job, "fetch_cluster_members", lambda ids: members_by_cluster)
    monkeypatch.setattr(job, "refresh_events", fake_refresh_raises)
    monkeypatch.setattr(job, "create_event", fake_create_event)

    generator = StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 이슈"))
    stats = asyncio.run(job._run(FakeExtractor(), generator))

    assert stats["refresh_failed"] == 1  # == len(refreshes)
    assert stats["refreshed"] == 0
    assert stats["created"] == 2
    assert job.summarize_stats(stats) == stats


def test_run_over_limit_caps_generator_calls(monkeypatch):
    """후보가 있는 신규 4개 중 상한 2개만 LLM 을 부른다."""

    promotable = [_cluster(cid) for cid in (1, 2, 3, 4)]
    members_by_cluster = {cid: _members() for cid in (1, 2, 3, 4)}

    async def fake_existing(cluster_ids):
        return set()

    async def fake_create_event(record):
        return 1

    monkeypatch.setattr(job, "get_event_settings", lambda: _event_settings(max_items_per_run=2))
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "fetch_promotable_clusters", lambda min_size, since: promotable)
    monkeypatch.setattr(job, "fetch_existing_event_ids", fake_existing)
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {})
    monkeypatch.setattr(job, "fetch_cluster_members", lambda ids: members_by_cluster)
    monkeypatch.setattr(job, "create_event", fake_create_event)

    generator = StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 이슈"))
    stats = asyncio.run(job._run(FakeExtractor(), generator))

    assert stats["skipped_over_limit"] == 2
    assert generator.calls == 2
    assert job.summarize_stats(stats) == stats


def test_run_empty_scan_returns_zero_stats_without_touching_neo4j(monkeypatch):
    """후보가 없으면 Neo4j 존재 조회조차 하지 않고 all-zero 로 반환한다."""

    called = {"existing": False}

    async def fake_existing(cluster_ids):
        called["existing"] = True
        return set()

    monkeypatch.setattr(job, "get_event_settings", lambda: _event_settings())
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "fetch_promotable_clusters", lambda min_size, since: [])
    monkeypatch.setattr(job, "fetch_existing_event_ids", fake_existing)

    generator = StubGenerator(EventDraft(companies=[], title="x"))
    stats = asyncio.run(job._run(FakeExtractor(), generator))

    assert stats == dict.fromkeys(job.STAT_KEYS, 0)
    assert called["existing"] is False
