"""Neo4j 참조 조회 — 이미 승격된 클러스터 id."""

from __future__ import annotations

from pipelines.common.clients.neo4j import neo4j_database


async def fetch_existing_event_ids(cluster_ids: list[int]) -> set[int]:
    """cluster_ids 중 Event 노드가 있는 것. 유니크 제약 인덱스로 한 번에 찾는다."""

    if not cluster_ids:
        return set()

    records = await neo4j_database.execute(
        """
        UNWIND $ids AS id
        MATCH (e:Event {cluster_id: id})
        RETURN e.cluster_id AS cluster_id
        """,
        {"ids": cluster_ids},
    )
    return {int(record["cluster_id"]) for record in records}
