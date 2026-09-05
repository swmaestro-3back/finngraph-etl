"""sync_events — 기존 Event 갱신 job. 모든 I/O 시임을 스텁으로 대체한다."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from pipelines.events.jobs import sync_events as job
from pipelines.events.models import ClusterCandidate

T0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


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


def _patch_common(monkeypatch, to_create, to_refresh):
    async def fake_scan(min_size, since):
        return to_create, to_refresh

    monkeypatch.setattr(job, "get_event_settings", lambda: _event_settings())
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "scan_promotable", fake_scan)


def test_run_refreshes_only_own_half(monkeypatch):
    """신규 몫(to_create)이 있어도 무시하고 기존 몫만 갱신한다."""

    captured = {}

    async def fake_refresh(refreshes):
        captured["refreshes"] = refreshes
        return len(refreshes)

    _patch_common(monkeypatch, to_create=[_cluster(9)], to_refresh=[_cluster(1), _cluster(2)])
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {i: [i * 100] for i in ids})
    monkeypatch.setattr(job, "refresh_events", fake_refresh)

    stats = asyncio.run(job._run())

    assert stats == {"scanned": 2, "refreshed": 2, "refresh_failed": 0}
    assert [r.cluster_id for r in captured["refreshes"]] == [1, 2]
    assert [r.news_ids for r in captured["refreshes"]] == [[100], [200]]
    assert job.summarize_stats(stats) == stats


def test_run_refresh_batch_failure_is_absorbed(monkeypatch):
    async def fake_refresh_raises(refreshes):
        raise RuntimeError("neo4j 다운")

    _patch_common(monkeypatch, to_create=[], to_refresh=[_cluster(1), _cluster(2), _cluster(3)])
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {})
    monkeypatch.setattr(job, "refresh_events", fake_refresh_raises)

    stats = asyncio.run(job._run())

    assert stats == {"scanned": 3, "refreshed": 0, "refresh_failed": 3}
    assert job.summarize_stats(stats) == stats


def test_run_partial_refresh_counts_missing_rows_as_failed(monkeypatch):
    async def fake_refresh(refreshes):
        return len(refreshes) - 1

    _patch_common(monkeypatch, to_create=[], to_refresh=[_cluster(1), _cluster(2)])
    monkeypatch.setattr(job, "fetch_cluster_news_ids", lambda ids: {})
    monkeypatch.setattr(job, "refresh_events", fake_refresh)

    stats = asyncio.run(job._run())

    assert stats == {"scanned": 2, "refreshed": 1, "refresh_failed": 1}


def test_run_nothing_to_refresh_returns_zero_stats_without_rdb_or_writes(monkeypatch):
    called = {"news_ids": False, "refresh": False}

    async def fake_refresh(refreshes):
        called["refresh"] = True
        return 0

    def fake_news_ids(ids):
        called["news_ids"] = True
        return {}

    _patch_common(monkeypatch, to_create=[_cluster(9)], to_refresh=[])
    monkeypatch.setattr(job, "fetch_cluster_news_ids", fake_news_ids)
    monkeypatch.setattr(job, "refresh_events", fake_refresh)

    stats = asyncio.run(job._run())

    assert stats == dict.fromkeys(job.STAT_KEYS, 0)
    assert called == {"news_ids": False, "refresh": False}


def test_summarize_stats_invariant():
    assert job.summarize_stats({"scanned": 3, "refreshed": 2, "refresh_failed": 1})
    with pytest.raises(ValueError):
        job.summarize_stats({"scanned": 3, "refreshed": 1, "refresh_failed": 1})
