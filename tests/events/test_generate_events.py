"""generate_events job 단위 테스트. RDB·Neo4j 는 갈아끼운다. LLM 은 부르지 않는다."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from pipelines.events.jobs import generate_events as job
from pipelines.events.models import ClusterCandidate

T0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
ZERO = {
    "scanned": 0,
    "created": 0,
    "created_without_edges": 0,
    "skipped_no_companies": 0,
    "failed": 0,
    "existing": 0,
    "updated": 0,
    "update_failed": 0,
}


class _StubNeo4jDatabase:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _cluster(cluster_id: int) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id,
        title=f"사건 {cluster_id}",
        first_published_at=T0,
        last_published_at=T0 + timedelta(days=2),
    )


def _patch_common(monkeypatch, clusters, company_ids, existing=()):
    seen = {}

    async def fake_scan(promote_size, since):
        seen["promote_size"] = promote_size
        return clusters, list(existing)

    async def no_update(clusters):
        raise AssertionError("기존 노드가 없는데 갱신을 호출함")

    def fake_company_ids(cluster_ids, min_articles):
        seen["company_args"] = (cluster_ids, min_articles)
        return company_ids

    monkeypatch.setattr(
        job, "get_event_settings", lambda: SimpleNamespace(scan_days=15, company_min_articles=3)
    )
    monkeypatch.setattr(job, "get_news_settings", lambda: SimpleNamespace(cluster_promote_size=10))
    monkeypatch.setattr(job, "neo4j_database", _StubNeo4jDatabase())
    monkeypatch.setattr(job, "scan_events", fake_scan)
    monkeypatch.setattr(job, "fetch_cluster_company_ids", fake_company_ids)
    monkeypatch.setattr(job, "update_last_published", no_update)
    return seen


def test_run_without_candidates_reads_nothing_else(monkeypatch):
    seen = _patch_common(monkeypatch, [], {})
    monkeypatch.setattr(
        job,
        "fetch_cluster_company_ids",
        lambda ids, min_articles: (_ for _ in ()).throw(
            AssertionError("후보가 없는데 당사자를 조회함")
        ),
    )

    assert asyncio.run(job._run()) == ZERO
    assert seen["promote_size"] == 10


def test_run_creates_events_with_company_ids_from_news_companies(monkeypatch):
    records = []

    async def fake_create_event(record):
        records.append(record)
        return len(record.company_ids)

    seen = _patch_common(monkeypatch, [_cluster(1), _cluster(2)], {1: [100, 300], 2: [500]})
    monkeypatch.setattr(job, "create_event", fake_create_event)

    stats = asyncio.run(job._run())

    assert stats == dict(ZERO, scanned=2, created=2)
    assert [(r.cluster_id, r.title, r.company_ids) for r in records] == [
        (1, "사건 1", [100, 300]),
        (2, "사건 2", [500]),
    ]
    assert records[0].first_published_at == T0
    assert records[0].last_published_at == T0 + timedelta(days=2)
    # 당사자 기준(후보 중 3건 이상)이 조회까지 내려간다
    assert seen["company_args"] == ([1, 2], 3)


def test_run_skips_clusters_without_companies(monkeypatch):
    """당사자가 0개면 노드를 만들지 않는다 — 다음 런에 다시 본다."""
    records = []

    async def fake_create_event(record):
        records.append(record)
        return 1

    _patch_common(monkeypatch, [_cluster(1), _cluster(2)], {2: [500]})
    monkeypatch.setattr(job, "create_event", fake_create_event)

    stats = asyncio.run(job._run())

    assert stats == dict(ZERO, scanned=2, created=1, skipped_no_companies=1)
    assert [r.cluster_id for r in records] == [2]


def test_run_counts_created_without_edges(monkeypatch):
    """기업은 있지만 그래프에 Company 노드가 없으면 Event 노드만 생긴다."""

    async def fake_create_event(record):
        return 0

    _patch_common(monkeypatch, [_cluster(1)], {1: [999]})
    monkeypatch.setattr(job, "create_event", fake_create_event)

    assert asyncio.run(job._run()) == dict(ZERO, scanned=1, created=1, created_without_edges=1)


def test_run_isolates_failures(monkeypatch):
    async def fake_create_event(record):
        if record.cluster_id == 1:
            raise RuntimeError("neo4j down")
        return 1

    _patch_common(monkeypatch, [_cluster(1), _cluster(2)], {1: [100], 2: [500]})
    monkeypatch.setattr(job, "create_event", fake_create_event)

    stats = asyncio.run(job._run())

    assert stats == dict(ZERO, scanned=2, created=1, failed=1)
    assert job.summarize_stats(stats) == stats


def test_run_updates_existing_events_without_creating(monkeypatch):
    """노드가 있는 클러스터는 last_published_at 만 올린다. 당사자를 조회하지 않는다."""
    updated_ids = []

    async def fake_update(clusters):
        updated_ids.extend(cluster.cluster_id for cluster in clusters)
        return 1

    _patch_common(monkeypatch, [], {}, existing=[_cluster(7), _cluster(8)])
    monkeypatch.setattr(job, "update_last_published", fake_update)
    monkeypatch.setattr(
        job,
        "fetch_cluster_company_ids",
        lambda ids, min_articles: (_ for _ in ()).throw(
            AssertionError("새 클러스터가 없는데 조회함")
        ),
    )

    assert asyncio.run(job._run()) == dict(ZERO, existing=2, updated=1)
    assert updated_ids == [7, 8]


def test_run_creates_new_and_updates_existing_together(monkeypatch):
    created = []

    async def fake_create_event(record):
        created.append(record.cluster_id)
        return 1

    async def fake_update(clusters):
        return len(clusters)

    _patch_common(monkeypatch, [_cluster(1)], {1: [100]}, existing=[_cluster(2)])
    monkeypatch.setattr(job, "create_event", fake_create_event)
    monkeypatch.setattr(job, "update_last_published", fake_update)

    assert asyncio.run(job._run()) == dict(ZERO, scanned=1, created=1, existing=1, updated=1)
    assert created == [1]


def test_run_isolates_update_failure_from_creation(monkeypatch):
    """갱신이 실패해도 생성은 돈다. 실패한 노드는 다음 런의 스캔이 다시 잡는다."""

    async def fake_create_event(record):
        return 1

    async def failing_update(clusters):
        raise RuntimeError("neo4j down")

    _patch_common(monkeypatch, [_cluster(1)], {1: [100]}, existing=[_cluster(2), _cluster(3)])
    monkeypatch.setattr(job, "create_event", fake_create_event)
    monkeypatch.setattr(job, "update_last_published", failing_update)

    stats = asyncio.run(job._run())

    assert stats == dict(ZERO, scanned=1, created=1, existing=2, update_failed=2)
    assert job.summarize_stats(stats) == stats


def test_summarize_stats_rejects_inconsistent_totals():
    with pytest.raises(ValueError):
        job.summarize_stats(dict(ZERO, scanned=3, created=1, failed=1))
