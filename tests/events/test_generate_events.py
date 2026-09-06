"""generate_events — 신규 Event 생성 job. 스텁 generator/loader 로 실패 격리와 집계."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from flashtext import KeywordProcessor

from pipelines.events.jobs import generate_events as job
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


class CountingFactory:
    """팩토리 호출 횟수를 센다 — 지연 생성(조용한 런에서 안 만듦) 검증용."""

    def __init__(self, instance):
        self.instance = instance
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.instance


def _cluster(cluster_id: int = 1) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id,
        representative_news_id=11,
        keywords=["삼성전자"],
        original_size=6,
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


# ---- 순수 헬퍼 ------------------------------------------------------------------


def test_build_create_input_dates_and_candidates():
    dated_texts, candidates = job.build_create_input(_members(), FakeExtractor(), lead_chars=600)

    assert dated_texts == [
        (date(2026, 9, 1), "삼성전자 유상증자 결정\n삼성전자이 결의했다."),
        (None, "삼성전자 주가 급락\n유증 소식에 급락. 기아 언급"),
    ]
    assert candidates == ["삼성전자", "기아"]


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


def test_summarize_stats_invariant():
    stats = {
        "scanned": 5,
        "created": 2,
        "created_without_edges": 1,
        "skipped_no_candidates": 1,
        "skipped_over_limit": 1,
        "failed": 1,
    }
    assert job.summarize_stats(dict(stats)) == stats

    with pytest.raises(ValueError):
        job.summarize_stats({**stats, "failed": 0})


# ---- create_one --------------------------------------------------------------------


def _create_one(monkeypatch, generator, fake_create_event, candidates=("삼성전자",)):
    monkeypatch.setattr(job, "create_event", fake_create_event)
    return asyncio.run(
        job.create_one(
            _cluster(),
            _members(),
            [(None, "t\nb")],
            list(candidates),
            generator,
            asyncio.Semaphore(1),
            title_max_chars=60,
        )
    )


def test_create_one_success_records_edges(monkeypatch):
    captured = {}

    async def fake_create_event(record):
        captured["record"] = record
        return 1

    generator = StubGenerator(
        EventDraft(companies=["삼성전자", "엔비디아"], title="[속보] 삼성전자 유상증자")
    )

    outcome = _create_one(monkeypatch, generator, fake_create_event, ("삼성전자", "기아"))

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

    generator = StubGenerator(EventDraft(companies=[], title="삼성전자 유상증자"))

    assert _create_one(monkeypatch, generator, fake_create_event) == "created_without_edges"


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

    assert _create_one(monkeypatch, generator, fake_create_event) == "failed"
    assert called["n"] == 0


def test_create_one_loader_failure(monkeypatch):
    async def fake_create_event(record):
        raise RuntimeError("neo4j down")

    generator = StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 유상증자"))

    assert _create_one(monkeypatch, generator, fake_create_event) == "failed"


# ---- _run 통합 (모든 I/O 시임을 스텁으로 대체) --------------------------------


class _StubNeo4jDatabase:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _event_settings(**overrides) -> SimpleNamespace:
    base = dict(
        min_size=5,
        scan_days=15,
        max_items_per_run=2,
        llm_max_concurrency=2,
        lead_chars=600,
        title_max_chars=60,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _patch_common(monkeypatch, to_create, to_refresh, members_by_cluster, settings=None):
    async def fake_scan(min_size, since):
        return to_create, to_refresh

    monkeypatch.setattr(job, "get_event_settings", lambda: settings or _event_settings())
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "scan_promotable", fake_scan)
    monkeypatch.setattr(job, "fetch_cluster_members", lambda ids: members_by_cluster)


def test_run_creates_only_own_half(monkeypatch):
    """기존 몫(to_refresh)은 무시. 신규 3(후보0 1, 유효 2 → 간선1/간선0)."""

    edges_by_cluster = {4: 0, 5: 1}

    async def fake_create_event(record):
        return edges_by_cluster[record.cluster_id]

    _patch_common(
        monkeypatch,
        to_create=[_cluster(3), _cluster(4), _cluster(5)],
        to_refresh=[_cluster(1), _cluster(2)],
        members_by_cluster={3: _no_candidate_members(), 4: _members(), 5: _members()},
    )
    monkeypatch.setattr(job, "create_event", fake_create_event)
    extractor_factory = CountingFactory(FakeExtractor())
    generator_factory = CountingFactory(
        StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 이슈"))
    )

    stats = asyncio.run(job._run(extractor_factory, generator_factory))

    assert stats == {
        "scanned": 3,
        "created": 2,
        "created_without_edges": 1,
        "skipped_no_candidates": 1,
        "skipped_over_limit": 0,
        "failed": 0,
    }
    assert job.summarize_stats(stats) == stats
    assert (extractor_factory.calls, generator_factory.calls) == (1, 1)


def test_run_over_limit_caps_generator_calls(monkeypatch):
    """후보가 있는 신규 4개 중 상한 2개만 LLM 을 부른다."""

    async def fake_create_event(record):
        return 1

    _patch_common(
        monkeypatch,
        to_create=[_cluster(cid) for cid in (1, 2, 3, 4)],
        to_refresh=[],
        members_by_cluster={cid: _members() for cid in (1, 2, 3, 4)},
        settings=_event_settings(max_items_per_run=2),
    )
    monkeypatch.setattr(job, "create_event", fake_create_event)
    generator = StubGenerator(EventDraft(companies=["삼성전자"], title="삼성전자 이슈"))

    stats = asyncio.run(job._run(lambda: FakeExtractor(), lambda: generator))

    assert stats["skipped_over_limit"] == 2
    assert stats["created"] == 2
    assert generator.calls == 2
    assert job.summarize_stats(stats) == stats


def test_run_empty_scan_returns_zero_stats_without_building_anything(monkeypatch):
    """신규 몫이 없으면 extractor·generator 를 만들지 않고 all-zero 로 반환한다."""

    _patch_common(monkeypatch, to_create=[], to_refresh=[_cluster(1)], members_by_cluster={})
    extractor_factory = CountingFactory(FakeExtractor())
    generator_factory = CountingFactory(StubGenerator(EventDraft(companies=[], title="x")))

    stats = asyncio.run(job._run(extractor_factory, generator_factory))

    assert stats == dict.fromkeys(job.STAT_KEYS, 0)
    assert (extractor_factory.calls, generator_factory.calls) == (0, 0)


def test_run_no_eligible_builds_extractor_but_not_generator(monkeypatch):
    """후보가 전부 0 이면 gazetteer 는 돌지만 Bedrock 클라이언트(generator)는 만들지 않는다."""

    _patch_common(
        monkeypatch,
        to_create=[_cluster(3)],
        to_refresh=[],
        members_by_cluster={3: _no_candidate_members()},
    )
    extractor_factory = CountingFactory(FakeExtractor())
    generator_factory = CountingFactory(StubGenerator(EventDraft(companies=[], title="x")))

    stats = asyncio.run(job._run(extractor_factory, generator_factory))

    assert stats["scanned"] == 1
    assert stats["skipped_no_candidates"] == 1
    assert (extractor_factory.calls, generator_factory.calls) == (1, 0)
    assert job.summarize_stats(stats) == stats
