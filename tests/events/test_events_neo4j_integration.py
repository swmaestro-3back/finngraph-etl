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
from pipelines.events.loaders.neo4j import create_event, refresh_events
from pipelines.events.models import EventRecord, EventRefresh
from pipelines.events.references.graph import fetch_existing_event_ids
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not get_settings().neo4j_uri, reason="NEO4J_URI 미설정(CI 등) — 로컬 Neo4j 필요"
    ),
]

T0 = datetime(2026, 9, 1, 9, 0, tzinfo=SEOUL_TIMEZONE)


def _record(cluster_id: int, companies: list[str], **overrides) -> EventRecord:
    base = dict(
        cluster_id=cluster_id,
        member_count=2,
        original_size=3,
        first_published_at=T0,
        last_published_at=T0 + timedelta(hours=2),
        representative_news_id=11,
        news_ids=[11, 12],
        keywords=["삼성전자", "유상증자"],
        title="삼성전자 4조원 유상증자",
        companies=companies,
    )
    base.update(overrides)
    return EventRecord(**base)


async def _read_event(cluster_id: int) -> dict:
    records = await neo4j_database.execute(
        """
        MATCH (e:Event {cluster_id: $cluster_id})
        OPTIONAL MATCH (c:Company)-[:HAS_EVENT]->(e)
        RETURN e.title AS title, e.companies AS companies, e.member_count AS member_count,
               e.original_size AS original_size, e.news_ids AS news_ids,
               e.representative_news_id AS representative_news_id,
               e.last_published_at AS last_published_at,
               e.titled_at AS titled_at, e.synced_at AS synced_at,
               collect(c.name) AS linked
        """,
        {"cluster_id": cluster_id},
    )
    assert len(records) == 1
    return dict(records[0])


def test_create_refresh_and_lookup():
    marker = uuid.uuid4().hex[:8]
    listed, unlisted, late = f"상장사{marker}", f"비상장{marker}", f"늦게시드{marker}"
    # 실제 클러스터 id 와 겹치지 않게 큰 값을 쓴다
    cluster_id = 10**9 + int(marker[:6], 16)

    async def _run() -> None:
        async with neo4j_database:
            try:
                await neo4j_database.execute(
                    """
                    CREATE (:Company {name: $listed, is_listed: true}),
                           (:Company {name: $unlisted, is_listed: false})
                    """,
                    {"listed": listed, "unlisted": unlisted},
                )

                # 존재 조회: 아직 없다
                assert await fetch_existing_event_ids([cluster_id]) == set()
                assert await fetch_existing_event_ids([]) == set()

                # 생성: is_listed 인 기업에만 간선. 미시드(late)·비상장은 간선 없음, 노드도 안 만듦
                edges = await create_event(_record(cluster_id, [listed, unlisted, late]))
                assert edges == 1
                row = await _read_event(cluster_id)
                assert row["title"] == "삼성전자 4조원 유상증자"
                assert row["companies"] == [listed, unlisted, late]
                assert row["linked"] == [listed]
                assert row["news_ids"] == [11, 12]
                assert row["titled_at"] is not None and row["synced_at"] is not None
                assert row["last_published_at"].to_native() == T0 + timedelta(hours=2)

                # 같은 입력 재실행 — 노드·간선 그대로 하나
                assert await create_event(_record(cluster_id, [listed, unlisted, late])) == 1
                count = await neo4j_database.execute(
                    "MATCH (e:Event {cluster_id: $id}) RETURN count(e) AS n", {"id": cluster_id}
                )
                assert count[0]["n"] == 1

                assert await fetch_existing_event_ids([cluster_id, cluster_id + 1]) == {cluster_id}

                # 늦게 시드된 기업이 생기면 갱신이 간선을 붙인다. 제목은 그대로
                await neo4j_database.execute(
                    "CREATE (:Company {name: $late, is_listed: true})", {"late": late}
                )
                refreshed = await refresh_events(
                    [
                        EventRefresh(
                            cluster_id=cluster_id,
                            member_count=3,
                            original_size=6,
                            first_published_at=T0,
                            last_published_at=T0 + timedelta(days=1),
                            representative_news_id=12,
                            news_ids=[11, 12, 13],
                            keywords=["삼성전자"],
                        )
                    ]
                )
                assert refreshed == 1
                row = await _read_event(cluster_id)
                assert row["title"] == "삼성전자 4조원 유상증자"
                assert (row["member_count"], row["original_size"]) == (3, 6)
                assert row["news_ids"] == [11, 12, 13]
                assert row["representative_news_id"] == 12
                assert sorted(row["linked"]) == sorted([listed, late])

                assert await refresh_events([]) == 0

                # 해석 0 이어도 노드는 생긴다
                other_id = cluster_id + 1
                assert await create_event(_record(other_id, ["없는기업" + marker])) == 0
                assert (await _read_event(other_id))["linked"] == []
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
