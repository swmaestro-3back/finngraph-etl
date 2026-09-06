"""Neo4j Event 조회용"""

from __future__ import annotations

from pipelines.common.clients.neo4j import neo4j_database


async def fetch_existing_event_ids(cluster_ids: list[int]) -> set[int]:
    """cluster_ids 중 이미 Event 노드로 승격된 cluster_id 조회"""

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
