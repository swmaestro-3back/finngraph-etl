"""승격 후보 스캔 — RDB 후보 조회 → Neo4j 존재 조회 → 기존/신규 분기.

sync_events 와 generate_events 가 같은 조건·같은 순서로 후보를 골라야 하므로 한 곳에 둔다.
두 job 이 각자 호출해 자기 몫(기존 Event = sync, 없는 클러스터 = generate)만 취한다 —
task 사이에 XCom 을 두지 않기 위한 선택이다. 읽기만 하므로 references/ 에 둔다.

호출자는 `async with neo4j_database:` 를 열고 있어야 한다.
"""

from __future__ import annotations

from datetime import datetime

from pipelines.events.models import ClusterCandidate
from pipelines.events.references.graph import fetch_existing_event_ids
from pipelines.events.references.rdb import fetch_promotable_clusters
from pipelines.events.transformers.planner import plan_actions


async def scan_promotable(
    min_size: int, since: datetime
) -> tuple[list[ClusterCandidate], list[ClusterCandidate]]:
    """(to_create, to_refresh). RDB 후보가 없으면 Neo4j 를 조회하지 않고 ([], []) 다."""

    clusters = fetch_promotable_clusters(min_size, since)
    if not clusters:
        return [], []

    existing = await fetch_existing_event_ids([c.cluster_id for c in clusters])
    return plan_actions(clusters, existing)
