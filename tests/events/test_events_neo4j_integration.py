"""Event 노드·HAS_EVENT 간선 적재 통합 테스트.

로컬 Neo4j 필요: docker compose up -d neo4j neo4j-init 후 `pytest -m integration`.
선례: tests/integration/test_edge_sync_neo4j_integration.py (uuid 마커 + DETACH DELETE 정리).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta

import pytest

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.config import get_settings
from pipelines.events.models import EventRecord
from pipelines.events.repositories.neo4j.events import (
    create_event,
    fetch_existing_event_ids,
    update_last_published,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not get_settings().neo4j_uri, reason="NEO4J_URI 미설정(CI 등) — 로컬 Neo4j 필요"
    ),
]

T0 = datetime(2026, 9, 1, 9, 0, tzinfo=SEOUL_TIMEZONE)
T_LAST = T0 + timedelta(days=2)


def _record(cluster_id: int, company_ids: list[int], **overrides) -> EventRecord:
    base = dict(
        cluster_id=cluster_id,
        title="삼성전자 4조원 유상증자",
        first_published_at=T0,
        last_published_at=T_LAST,
        company_ids=company_ids,
    )
    base.update(overrides)
    return EventRecord(**base)


async def _read_event(cluster_id: int) -> dict:
    records = await neo4j_database.execute(
        """
        MATCH (e:Event {cluster_id: $cluster_id})
        OPTIONAL MATCH (c:Company)-[:HAS_EVENT]->(e)
        RETURN e.title AS title, e.first_published_at AS first_published_at,
               e.last_published_at AS last_published_at, e.created_at AS created_at, keys(e) AS keys, collect(c.name) AS linked
        """,
        {"cluster_id": cluster_id},
    )
    assert len(records) == 1
    return dict(records[0])


def test_create_and_lookup():
    marker = uuid.uuid4().hex[:8]
    listed, unlisted, late = f"상장사{marker}", f"비상장{marker}", f"늦게시드{marker}"
    # 실제 클러스터·기업 id 와 겹치지 않게 큰 값을 쓴다
    cluster_id = 10**9 + int(marker[:6], 16)
    listed_id, unlisted_id, late_id = (cluster_id + offset for offset in range(3))
    ids = [listed_id, unlisted_id, late_id]

    async def _run() -> None:
        async with neo4j_database:
            try:
                await neo4j_database.execute(
                    """
                    CREATE (:Company {name: $listed, company_id: $listed_id, is_listed: true}),
                           (:Company {name: $unlisted, company_id: $unlisted_id, is_listed: false})
                    """,
                    {
                        "listed": listed,
                        "listed_id": listed_id,
                        "unlisted": unlisted,
                        "unlisted_id": unlisted_id,
                    },
                )

                assert await fetch_existing_event_ids([cluster_id]) == set()
                assert await fetch_existing_event_ids([]) == set()

                # company_id 로 찾은 is_listed 기업에만 간선. 미시드(late)·비상장은 간선 없음
                assert await create_event(_record(cluster_id, ids)) == 1
                row = await _read_event(cluster_id)
                assert row["title"] == "삼성전자 4조원 유상증자"
                assert row["linked"] == [listed]
                assert row["first_published_at"].to_native() == T0
                assert row["last_published_at"].to_native() == T_LAST
                # 노드에는 그래프에 보여 줄 값만 있다
                assert sorted(row["keys"]) == [
                    "cluster_id",
                    "created_at",
                    "first_published_at",
                    "last_published_at",
                    "title",
                ]
                created_at = row["created_at"]

                # 재실행 — 노드는 하나이고 값이 바뀌지 않는다. 그 사이 시드된 기업은 간선이 붙는다.
                await neo4j_database.execute(
                    "CREATE (:Company {name: $late, company_id: $late_id, is_listed: true})",
                    {"late": late, "late_id": late_id},
                )
                assert await create_event(_record(cluster_id, ids, title="바뀐 제목")) == 2
                row = await _read_event(cluster_id)
                assert row["title"] == "삼성전자 4조원 유상증자"
                assert row["created_at"] == created_at
                # 재생성은 last_published_at 도 바꾸지 않는다 — 갱신은 update_last_published 몫이다
                assert row["last_published_at"].to_native() == T_LAST
                assert sorted(row["linked"]) == sorted([listed, late])
                count = await neo4j_database.execute(
                    "MATCH (e:Event {cluster_id: $id}) RETURN count(e) AS n", {"id": cluster_id}
                )
                assert count[0]["n"] == 1

                # 그래프에 없는 기업뿐이어도 노드는 생긴다
                assert await create_event(_record(cluster_id + 1, [-1])) == 0
                assert await fetch_existing_event_ids([cluster_id, cluster_id + 1, 1]) >= {
                    cluster_id,
                    cluster_id + 1,
                }
            finally:
                await neo4j_database.execute(
                    "MATCH (e:Event) WHERE e.cluster_id IN $ids DETACH DELETE e",
                    {"ids": [cluster_id, cluster_id + 1]},
                )
                await neo4j_database.execute(
                    "MATCH (c:Company) WHERE c.name IN $names DETACH DELETE c",
                    {"names": [listed, unlisted, late]},
                )

    asyncio.run(_run())


def test_update_last_published_only_moves_forward():
    marker = uuid.uuid4().hex[:8]
    cluster_id = 2 * 10**9 + int(marker[:6], 16)
    later = T_LAST + timedelta(days=3)

    def candidate(cluster: int, last: datetime) -> EventRecord:
        return _record(cluster, [], last_published_at=last)

    async def _last(cluster: int) -> datetime:
        return (await _read_event(cluster))["last_published_at"].to_native()

    async def _run() -> None:
        async with neo4j_database:
            try:
                await create_event(_record(cluster_id, []))
                assert await update_last_published([]) == 0

                # 늦은 값은 반영한다. 다른 속성은 그대로다
                assert await update_last_published([candidate(cluster_id, later)]) == 1
                row = await _read_event(cluster_id)
                assert row["last_published_at"].to_native() == later
                assert row["title"] == "삼성전자 4조원 유상증자"
                assert row["first_published_at"].to_native() == T0

                # 같거나 이른 값은 쓰지 않는다
                assert await update_last_published([candidate(cluster_id, later)]) == 0
                assert await update_last_published([candidate(cluster_id, T_LAST)]) == 0
                assert await _last(cluster_id) == later

                # 노드가 없는 cluster_id 는 만들지 않고 건너뛴다
                assert await update_last_published([candidate(cluster_id + 1, later)]) == 0
                assert await fetch_existing_event_ids([cluster_id + 1]) == set()
            finally:
                await neo4j_database.execute(
                    "MATCH (e:Event) WHERE e.cluster_id IN $ids DETACH DELETE e",
                    {"ids": [cluster_id, cluster_id + 1]},
                )

    asyncio.run(_run())
