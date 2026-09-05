"""후보 클러스터를 생성(LLM)과 갱신(카운터 덮어쓰기)으로 가른다.

비교 키가 없다 — 노드가 있으면 무조건 갱신한다(spec §2-10). 생성 목록은 타임라인에 먼저
필요한 최신 순으로 정렬해, job 이 상한을 앞에서부터 자를 수 있게 한다. SQL ORDER BY 에
기대지 않는다.
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
