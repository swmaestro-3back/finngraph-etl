"""Event 노드·HAS_EVENT 간선 조회·적재."""

from __future__ import annotations

from datetime import datetime, timezone

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.utils.time import now_kst
from pipelines.events.models import ClusterCandidate, EventRecord


def _bolt_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or isinstance(value.tzinfo, timezone):
        return value
    return value.astimezone(timezone(value.utcoffset()))


def _bolt_params(payload: dict) -> dict:
    """딕셔너리 값 중 datetime 만 _bolt_datetime 으로 바꾼 새 dict."""

    return {
        key: _bolt_datetime(value) if isinstance(value, datetime) else value
        for key, value in payload.items()
    }


# 노드에는 그래프에 보여 줄 값만 둔다. 키워드·기사 목록·기사 수는 cluster_id 로 RDB 에서 읽는다.
# ON CREATE 라 다시 실행해도 값이 바뀌지 않고, 간선만 그 사이 시드된 기업에 새로 붙는다.
# 생성 뒤 바뀌는 값은 last_published_at 하나이고 UPDATE_LAST_PUBLISHED_CYPHER 가 갱신한다.
CREATE_EVENT_CYPHER = """
MERGE (e:Event {cluster_id: $cluster_id})
ON CREATE SET e.title = $title,
              e.first_published_at = $first_published_at,
              e.last_published_at = $last_published_at,
              e.created_at = $now
WITH e
CALL {
    WITH e
    UNWIND $company_ids AS company_id
    MATCH (c:Company {company_id: company_id})
    WHERE c.is_listed = true
    MERGE (c)-[:HAS_EVENT]->(e)
    RETURN count(c) AS edges
}
RETURN edges
"""


# 승격 뒤 기사가 붙어 늘어난 last_published_at 만 반영한다. 값이 커질 때만 쓴다 — 늦게 돈 런이
# 옛 값으로 되돌리지 않는다. 노드가 없는 cluster_id 는 건너뛴다.
UPDATE_LAST_PUBLISHED_CYPHER = """
UNWIND $rows AS row
MATCH (e:Event {cluster_id: row.cluster_id})
WHERE e.last_published_at IS NULL OR e.last_published_at < row.last_published_at
SET e.last_published_at = row.last_published_at
RETURN count(e) AS updated
"""


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


async def create_event(record: EventRecord) -> int:
    """Event 노드를 만들고 당사자 기업과 잇는다. 이번에 확인된 간선 수를 돌려준다."""

    records = await neo4j_database.execute(
        CREATE_EVENT_CYPHER, _bolt_params({**record.model_dump(), "now": now_kst()})
    )
    return int(records[0]["edges"]) if records else 0


async def update_last_published(clusters: list[ClusterCandidate]) -> int:
    """기존 Event 노드의 last_published_at 을 RDB 값으로 올린다. 실제로 바뀐 노드 수를 돌려준다."""

    if not clusters:
        return 0

    records = await neo4j_database.execute(
        UPDATE_LAST_PUBLISHED_CYPHER,
        {
            "rows": [
                {
                    "cluster_id": cluster.cluster_id,
                    "last_published_at": _bolt_datetime(cluster.last_published_at),
                }
                for cluster in clusters
            ]
        },
    )
    return int(records[0]["updated"]) if records else 0
