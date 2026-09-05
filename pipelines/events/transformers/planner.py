"""
클러스터를 새 EVENT 생성과 기존 EVENT 갱신으로 분리
"""

from __future__ import annotations

from pipelines.events.models import ClusterCandidate


def plan_actions(
    clusters: list[ClusterCandidate], existing_ids: set[int]
) -> tuple[list[ClusterCandidate], list[ClusterCandidate]]:
    to_create = sorted(
        (c for c in clusters if c.cluster_id not in existing_ids),
        key=lambda c: (c.last_published_at, c.cluster_id),
        reverse=True,
    )
    to_refresh = [c for c in clusters if c.cluster_id in existing_ids]
    return to_create, to_refresh
