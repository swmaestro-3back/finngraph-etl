from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.triples.edges import source_row_of
from pipelines.triples.nodes.entity_extractor import linked_entities
from pipelines.triples.repositories.neo4j.relations import sync_edge_summaries
from pipelines.triples.repositories.postgres.entities_relations import fetch_edge_summaries
from pipelines.triples.repositories.postgres.news import (
    fetch_unprocessed_triple_news_items,
    mark_triple_extraction_result,
)
from pipelines.triples.repositories.postgres.relation_sources import insert_relation_sources
from pipelines.triples.workflow import MIN_COMPANIES, GraphRunner

logger = get_logger(__name__)


async def _process_item(runner: GraphRunner, item: dict[str, Any]) -> str:
    """
    1. news_companies 의 연결 기업으로 엔티티 조립 (LLM 없음). 기업이 2개 미만이면 추출 없이 마킹
    2. LangGraph Runner 실행 (관계 추출 → 주석 → 삼중항)
    3. relation_sources(근거 원장)에 뉴스 근거 적재
    4. entities_relations 뷰 기준으로 Neo4j 간선 요약 동기화
    5. News 테이블에 triple_extracted 마킹

    news_companies 는 쓰지 않는다 — 수집 단계가 저장과 함께 연결한다.
    """
    news_id = item["news_id"]

    try:
        entities = linked_entities(item["text"], item["company_ids"])
        if len({entity.company_id for entity in entities}) < MIN_COMPANIES:
            mark_triple_extraction_result([], [news_id])
            return "no_triples"

        # 트리플추출
        final_state = await runner.ainvoke(str(news_id), item["text"], entities)
        triplets = final_state.get("triplets") or []

        if triplets:
            # 같은 뉴스 안에서 여러 문장이 같은 삼중항으로 수렴하면 첫 문장만 남긴다.
            source_rows: list[dict] = []
            seen_keys: set[tuple[str, str, str]] = set()
            for triplet in triplets:
                row = source_row_of(triplet)
                if row is None:
                    continue
                key = (row["subject_name"], row["relation"], row["object_name"])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                source_rows.append(row)

            # 원장 먼저, 그래프는 원장 집계의 캐시 — 순서가 정합성의 근거다.
            insert_relation_sources(news_id, item["mentioned_at"], source_rows)
            summaries = fetch_edge_summaries(sorted(seen_keys))
            await sync_edge_summaries(summaries)

        has_triplets = bool(triplets)
        mark_triple_extraction_result(
            [news_id] if has_triplets else [],
            [] if has_triplets else [news_id],
        )
    except Exception as e:
        logger.warning(
            "[extract_triples] 추출 실패 (다음 런 재시도): news_id=%s, %s: %s",
            news_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "has_triples" if has_triplets else "no_triples"


async def extract_unprocessed_triples() -> dict[str, int]:
    """
    삼중항 미처리 클러스터 대표 기사를 전량 폴링해 트리플을 추출하고 원장·그래프에 적재
    """

    # 승격 기준은 news 설정의 NEWS_CLUSTER_PROMOTE_SIZE 를 그대로 쓴다
    items = fetch_unprocessed_triple_news_items(get_news_settings().cluster_promote_size)

    stats = {"fetched": len(items), "has_triples": 0, "no_triples": 0, "failed": 0}

    if not items:
        logger.info("[extract_triples] 대상 뉴스 없음")
        return stats

    # 트리플관계 추출 LangGraph Runner 생성
    runner = GraphRunner()

    # 각 뉴스 하나씩 순차 실행
    async with neo4j_database:
        for item in items:
            status = await _process_item(runner, item)
            stats[status] += 1

    logger.info(
        "[extract_triples] 완료: 조회 %d / 관계있음 %d / 관계없음 %d / 실패 %d",
        stats["fetched"],
        stats["has_triples"],
        stats["no_triples"],
        stats["failed"],
    )

    return stats


def run() -> dict[str, int]:
    return asyncio.run(extract_unprocessed_triples())


if __name__ == "__main__":
    run()
