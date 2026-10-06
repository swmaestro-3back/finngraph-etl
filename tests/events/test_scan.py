"""scan_new_events — RDB 후보 조회 → Neo4j 존재 조회 → 없는 것만."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from pipelines.events import scan
from pipelines.events.models import ClusterCandidate

T0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def _cluster(cluster_id: int) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id, title=f"사건 {cluster_id}", first_published_at=T0
    )


def test_empty_scan_skips_neo4j_lookup(monkeypatch):
    called = {"existing": False}

    async def fake_existing(cluster_ids):
        called["existing"] = True
        return set()

    monkeypatch.setattr(scan, "fetch_promoted_clusters", lambda promote_size, since: [])
    monkeypatch.setattr(scan, "fetch_existing_event_ids", fake_existing)

    assert asyncio.run(scan.scan_new_events(10, T0)) == []
    assert called["existing"] is False


def test_returns_only_clusters_without_event(monkeypatch):
    seen = {}

    async def fake_existing(cluster_ids):
        seen["ids"] = cluster_ids
        return {2}

    def fake_promoted(promote_size, since):
        seen["args"] = (promote_size, since)
        return [_cluster(1), _cluster(2), _cluster(3)]

    monkeypatch.setattr(scan, "fetch_promoted_clusters", fake_promoted)
    monkeypatch.setattr(scan, "fetch_existing_event_ids", fake_existing)

    new = asyncio.run(scan.scan_new_events(10, T0))

    assert [cluster.cluster_id for cluster in new] == [1, 3]
    assert seen == {"ids": [1, 2, 3], "args": (10, T0)}
