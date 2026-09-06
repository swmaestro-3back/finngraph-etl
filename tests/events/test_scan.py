"""scan_promotable — RDB 후보 조회 → Neo4j 존재 조회 → 분기 배선."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from pipelines.events.models import ClusterCandidate
from pipelines.events.references import scan

T0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


def _cluster(cluster_id: int, hours: int = 0) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id,
        representative_news_id=None,
        keywords=[],
        original_size=5,
        member_count=2,
        first_published_at=T0,
        last_published_at=T0 + timedelta(hours=hours),
    )


def test_empty_scan_skips_neo4j_lookup(monkeypatch):
    called = {"existing": False}

    async def fake_existing(cluster_ids):
        called["existing"] = True
        return set()

    monkeypatch.setattr(scan, "fetch_promotable_clusters", lambda min_size, since: [])
    monkeypatch.setattr(scan, "fetch_existing_event_ids", fake_existing)

    assert asyncio.run(scan.scan_promotable(5, T0)) == ([], [])
    assert called["existing"] is False


def test_splits_by_existing_ids_and_sorts_create_newest_first(monkeypatch):
    promotable = [_cluster(1, 0), _cluster(2, 1), _cluster(3, 2)]
    seen = {}

    async def fake_existing(cluster_ids):
        seen["ids"] = cluster_ids
        return {2}

    monkeypatch.setattr(scan, "fetch_promotable_clusters", lambda min_size, since: promotable)
    monkeypatch.setattr(scan, "fetch_existing_event_ids", fake_existing)

    to_create, to_refresh = asyncio.run(scan.scan_promotable(5, T0))

    assert seen["ids"] == [1, 2, 3]
    assert [c.cluster_id for c in to_create] == [3, 1]
    assert [c.cluster_id for c in to_refresh] == [2]
