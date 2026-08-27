"""공급계약 공시를 근거 원장과 그래프 간선 요약에 반영한다.

disclosures 테이블에서 계약상대 ticker 매칭에 성공한 공시 전량을 읽어
relation_sources(근거 원장)의 disclosure 행으로 재구성한다 — upsert 와 함께 이번
원천에 없는 행(정정으로 상대가 바뀌었거나 상폐로 매칭에서 빠진 공시)을 삭제해야
전량 재적재다. 이어서 영향받은 간선(이전에 실려 있던 키 포함)의 요약을
entities_relations 뷰 기준으로 Neo4j에 동기화하고, 근거가 모두 사라진 간선은
그래프에서 지운다. 원천이 Postgres 라 매 실행이 멱등이다 — 수집 job 과 분리해 둔
덕에 매칭 로직이 바뀌어도 재수집 없이 이 job 만 다시 돌리면 원장과 그래프가 따라온다.
"""

from __future__ import annotations

import asyncio

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.disclosures.loaders.postgres import (
    delete_stale_relation_sources,
    fetch_disclosure_edge_keys,
    fetch_supply_edges,
    upsert_relation_sources,
)
from pipelines.triples.loaders.neo4j import delete_edges, sync_edge_summaries
from pipelines.triples.references.rdb import fetch_edge_summaries

logger = get_logger(__name__)


async def _link() -> None:
    with session_scope() as session:
        edges = fetch_supply_edges(session)

        # 원천이 비면 아무것도 회수하지 않고 끝낸다 — disclosures 는 append-only 라
        # 전량 소멸은 정상 경로가 아니고, 일시 장애로 빈 결과가 왔을 때 원장을
        # 전부 지우는 사고를 막는 보수적 가드다.
        if not edges:
            logger.info("근거 원장에 반영할 공급계약 공시가 없습니다.")
            return

        # 재구성 전에 실려 있던 키까지 동기화 대상에 넣어야, 이번 원천에서 빠진
        # 간선의 남은 요약값(disclosure_count 등)도 바로잡힌다.
        previous_keys = set(fetch_disclosure_edge_keys(session))
        upsert_relation_sources(session, edges)
        stale_count = delete_stale_relation_sources(session, edges)

    current_keys = {(edge.filer_name, "SUPPLIES_TO", edge.counterparty_name) for edge in edges}
    keys = sorted(previous_keys | current_keys)

    summaries = fetch_edge_summaries(keys)

    # 뷰에서 빠진 키 = 뉴스 근거도 없이 원장에서 사라진 간선 → 그래프에서 제거.
    remaining = {(s["subject_name"], s["relation"], s["object_name"]) for s in summaries}
    gone_keys = [key for key in keys if key not in remaining]

    async with neo4j_database:
        synced = await sync_edge_summaries(summaries)
        deleted = await delete_edges(gone_keys)

    logger.info(
        "공급계약 원장 반영: 공시 %d건 → 회사쌍 %d개, "
        "원장 정리 %d행, 간선 동기화 %d개, 간선 삭제 %d개",
        len(edges),
        len(current_keys),
        stale_count,
        synced,
        deleted,
    )


def run() -> None:
    asyncio.run(_link())
