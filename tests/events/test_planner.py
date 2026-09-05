"""존재 집합으로 생성/갱신을 가른다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pipelines.events.models import ClusterCandidate
from pipelines.events.transformers.planner import plan_actions

T0 = datetime(2026, 9, 1, 9, 0, tzinfo=UTC)


def _cluster(cluster_id: int, hours: int) -> ClusterCandidate:
    return ClusterCandidate(
        cluster_id=cluster_id,
        representative_news_id=None,
        keywords=[],
        original_size=2,
        member_count=1,
        first_published_at=T0,
        last_published_at=T0 + timedelta(hours=hours),
    )


def test_splits_by_existing_ids():
    clusters = [_cluster(1, 0), _cluster(2, 1), _cluster(3, 2)]

    to_create, to_refresh = plan_actions(clusters, existing_ids={2})

    assert [c.cluster_id for c in to_create] == [3, 1]
    assert [c.cluster_id for c in to_refresh] == [2]


def test_create_is_sorted_newest_first_even_if_input_is_shuffled():
    clusters = [_cluster(1, 1), _cluster(2, 5), _cluster(3, 3)]

    to_create, _ = plan_actions(clusters, existing_ids=set())

    assert [c.cluster_id for c in to_create] == [2, 3, 1]


def test_tie_breaks_by_cluster_id_desc():
    clusters = [_cluster(1, 0), _cluster(2, 0)]

    to_create, _ = plan_actions(clusters, existing_ids=set())

    assert [c.cluster_id for c in to_create] == [2, 1]


def test_empty():
    assert plan_actions([], existing_ids={1}) == ([], [])
