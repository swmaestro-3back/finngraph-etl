"""Event 노드·HAS_EVENT 간선 적재.

RDB 원장이 없는 첫 사례라(spec §2-6) 노드가 표시용 값을 직접 든다. 생성·갱신 모두
MERGE/SET 덮어쓰기라 몇 번을 다시 돌려도 같은 상태로 수렴한다.

간선 해석은 MATCH 다 — 없는 기업은 간선이 안 생기고 이름만 있는 노드도 만들지 않는다.
간선은 생성 때와 매 갱신 때 companies 로 다시 MERGE 해서, 생성 시점에 노드가 없던 기업
(나중에 시드된 KRX 신규 상장, 0002 적용 전 US)도 다음 갱신에서 붙는다. 삭제는 없다 —
상장폐지는 KRX 시드의 DETACH DELETE 가 간선까지 지운다.

모든 datetime 파라미터는 `_bolt_params`/`_bolt_datetime` 을 거쳐 고정 오프셋으로 바꿔
넘긴다 — ZoneInfo tzinfo 를 패킹하면 드라이버가 세그폴트를 낸다(아래 docstring 참고).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.utils.time import now_kst
from pipelines.events.models import EventRecord, EventRefresh


def _bolt_datetime(value: datetime) -> datetime:
    """가변 tzinfo(ZoneInfo 등)를 같은 순간·같은 벽시계의 고정 오프셋으로 바꾼다.

    neo4j 드라이버는 패킹 때 tzinfo.utcoffset() 에 자기 DateTime(datetime 이 아님)을 넘기고
    TypeError 폴백을 기대하는데, CPython 3.14 의 zoneinfo 는 TypeError 대신 세그폴트를 낸다.
    psycopg 가 TIMESTAMPTZ 를 ZoneInfo 로 돌려주므로 RDB 에서 온 값도 전부 여기를 거친다.
    naive 나 이미 고정 오프셋인 값은 그대로 둔다.
    """

    if value.tzinfo is None or isinstance(value.tzinfo, timezone):
        return value
    return value.astimezone(timezone(value.utcoffset()))


def _bolt_params(payload: dict) -> dict:
    """딕셔너리 값 중 datetime 만 _bolt_datetime 으로 바꾼 새 dict."""

    return {
        key: _bolt_datetime(value) if isinstance(value, datetime) else value
        for key, value in payload.items()
    }


# 간선 절을 CALL {} 에 넣는 이유: companies 가 비거나 해석이 0 이어도 바깥 행이 살아남아
# edges = 0 이 돌아온다. 기존 간선을 지우는 절은 없다 — 생성은 노드가 없을 때만 오고,
# 재시도로 두 번 와도 MERGE 라 같은 간선이다.
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
    """Event 노드를 만들고(있으면 덮어쓰고) 해석된 기업과 간선을 건다. 간선 수를 돌려준다."""

    records = await neo4j_database.execute(
        CREATE_EVENT_CYPHER, _bolt_params({**record.model_dump(), "now": now_kst()})
    )
    return int(records[0]["edges"]) if records else 0


async def refresh_events(refreshes: list[EventRefresh]) -> int:
    """카운터·시간 범위·대표·news_ids·keywords 를 덮어쓰고 간선을 다시 해석해 추가한다.

    title·companies 는 건드리지 않는다. 갱신된 노드 수를 돌려준다.
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
