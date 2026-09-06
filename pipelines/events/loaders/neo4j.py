"""Event 노드·HAS_EVENT 간선 적재."""

from __future__ import annotations

from datetime import datetime, timezone

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.utils.time import now_kst
from pipelines.events.models import EventRecord, EventRefresh


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


CREATE_EVENT_CYPHER = """
MERGE (e:Event {cluster_id: $cluster_id})
SET e.title = $title,
    e.companies = $companies,
    e.member_count = $member_count,
    e.original_size = $original_size,
    e.first_published_at = $first_published_at,
    e.last_published_at = $last_published_at,
    e.representative_news_id = $representative_news_id,
    e.news_ids = $news_ids,
    e.keywords = $keywords,
    e.titled_at = $now,
    e.synced_at = $now
WITH e
CALL {
    WITH e
    UNWIND $companies AS name
    MATCH (c:Company {name: name})
    WHERE c.is_listed = true
    MERGE (c)-[:HAS_EVENT]->(e)
    RETURN count(c) AS edges
}
RETURN edges
"""

REFRESH_EVENTS_CYPHER = """
UNWIND $rows AS row
MATCH (e:Event {cluster_id: row.cluster_id})
SET e.member_count = row.member_count,
    e.original_size = row.original_size,
    e.first_published_at = row.first_published_at,
    e.last_published_at = row.last_published_at,
    e.representative_news_id = row.representative_news_id,
    e.news_ids = row.news_ids,
    e.keywords = row.keywords,
    e.synced_at = $now
WITH e
CALL {
    WITH e
    UNWIND e.companies AS name
    MATCH (c:Company {name: name})
    WHERE c.is_listed = true
    MERGE (c)-[:HAS_EVENT]->(e)
    RETURN count(c) AS edges
}
RETURN count(e) AS refreshed
"""


async def create_event(record: EventRecord) -> int:
    """
    EVENT 노드 생성
    """

    records = await neo4j_database.execute(
        CREATE_EVENT_CYPHER, _bolt_params({**record.model_dump(), "now": now_kst()})
    )
    return int(records[0]["edges"]) if records else 0


async def refresh_events(refreshes: list[EventRefresh]) -> int:
    """
    EVENT 노드 업데이트
    title/companies는 바꾸지 않고,
    original_size, member_count, news_ids, published_at과 같은 메타데이터만 업데이트 한다
    """

    if not refreshes:
        return 0

    records = await neo4j_database.execute(
        REFRESH_EVENTS_CYPHER,
        {
            "rows": [_bolt_params(refresh.model_dump()) for refresh in refreshes],
            "now": _bolt_datetime(now_kst()),
        },
    )
    return int(records[0]["refreshed"]) if records else 0
