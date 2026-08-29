"""sync_edge_summaries Neo4j 통합 테스트 — 공시·뉴스 근거 배열의 간선 반영.

로컬 Neo4j 필요: docker compose up -d neo4j neo4j-init 후 `pytest -m integration`.
테스트 노드는 uuid 마커가 붙은 이름으로 만들고 끝나면 DETACH DELETE 로 지운다.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date

import pytest

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.triples.loaders.neo4j import sync_edge_summaries

pytestmark = pytest.mark.integration


def test_sync_edge_summaries_sets_disclosure_arrays() -> None:
    marker = uuid.uuid4().hex[:8]
    subject, obj = f"공급사{marker}", f"수요사{marker}"

    summary = {
        "subject_name": subject,
        "relation": "SUPPLIES_TO",
        "object_name": obj,
        "news_mention_count": 2,
        "disclosure_count": 2,
        "first_mentioned_at": date(2026, 8, 1),
        "last_mentioned_at": date(2026, 8, 21),
        "disclosure_rcept_nos": ["99999901000001", "99999901000002"],
        "disclosure_items": ["기타 판매ㆍ공급계약: 계약A", "계약B"],
        "news_ids": [101, 102],
        "news_items": ["카메라 모듈"],
    }

    async def _run() -> list:
        async with neo4j_database:
            try:
                assert await sync_edge_summaries([summary]) == 1
                return await neo4j_database.execute(
                    """
                    MATCH (s:Company {name: $subject})-[r:SUPPLIES_TO]->(o:Company {name: $object})
                    RETURN r.disclosure_count AS disclosure_count,
                           r.disclosure_rcept_nos AS rcept_nos,
                           r.disclosure_items AS items,
                           r.news_ids AS news_ids,
                           r.news_items AS news_items
                    """,
                    {"subject": subject, "object": obj},
                )
            finally:
                await neo4j_database.execute(
                    "MATCH (n:Company) WHERE n.name IN [$subject, $object] DETACH DELETE n",
                    {"subject": subject, "object": obj},
                )

    records = asyncio.run(_run())

    assert len(records) == 1
    assert records[0]["disclosure_count"] == 2
    assert records[0]["rcept_nos"] == ["99999901000001", "99999901000002"]
    assert records[0]["items"] == ["기타 판매ㆍ공급계약: 계약A", "계약B"]
    assert records[0]["news_ids"] == [101, 102]
    assert records[0]["news_items"] == ["카메라 모듈"]
