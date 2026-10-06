"""Event 후보 스캔 — RDB 후보 조회 → Neo4j 존재 조회 → 노드가 없는 것과 있는 것으로 나눈다.

두 저장소 조회(repositories/)를 합성하므로 저장소 계층이 아니라 파이프라인 루트에 둔다 —
repositories/ 는 한 저장소의 쿼리만 갖는다.

호출자는 `async with neo4j_database:` 를 열고 있어야 한다.
"""

from __future__ import annotations

from datetime import datetime

from pipelines.events.models import ClusterCandidate
from pipelines.events.repositories.neo4j.events import fetch_existing_event_ids
from pipelines.events.repositories.postgres.news_clusters import fetch_promoted_clusters


async def scan_events(
    promote_size: int, since: datetime
) -> tuple[list[ClusterCandidate], list[ClusterCandidate]]:
    """승격되고 제목이 붙은 클러스터를 (Event 노드가 없는 것, 있는 것) 으로 나눠 돌려준다.

    노드가 없는 것은 새로 만들고, 있는 것은 last_published_at 을 갱신한다. RDB 후보가 없으면
    Neo4j 를 조회하지 않는다.
    """

    clusters = fetch_promoted_clusters(promote_size, since)
    if not clusters:
        return [], []

    existing = await fetch_existing_event_ids([cluster.cluster_id for cluster in clusters])
    new = [cluster for cluster in clusters if cluster.cluster_id not in existing]
    found = [cluster for cluster in clusters if cluster.cluster_id in existing]
    return new, found
