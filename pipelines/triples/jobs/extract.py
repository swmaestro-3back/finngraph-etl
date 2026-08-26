from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.news.loaders.postgres import (
    fetch_unprocessed_triple_news_items,
    mark_triple_extraction_result,
)
from pipelines.triples.loaders.neo4j import upsert_triplets
from pipelines.triples.loaders.postgres import (
    insert_news_companies,
    insert_relation_sources,
)
from pipelines.triples.transformers.company_links import resolve_company_ids
from pipelines.triples.workflow import GraphRunner

logger = get_logger(__name__)

DEFAULT_LIMIT = 100


async def _process_item(runner: GraphRunner, item: dict[str, Any]) -> str:
    """
    1. LangGraph Runner 실행
    2. GraphDB에 트리플관계 반영 + relation_source(간선 출처)·news_companies(기업 매핑) 적재
    3. News 테이블에 is_processed / relation_extracted 마킹
    """
    news_id = item["news_id"]

    try:
        # 트리플추출
        final_state = await runner.ainvoke(str(news_id), item["text"])
        triplets = final_state.get("triplets") or []

        if triplets:
            # Neo4j 반영 후 간선 elementId를 받아 relation_source에 기록
            edge_rows = await upsert_triplets(str(news_id), triplets)
            insert_relation_sources(news_id, edge_rows)

            # 삼중항에 등장한 상장사를 news_companies에 연결
            company_ids = await resolve_company_ids(triplets)
            insert_news_companies(news_id, company_ids)

        has_triplets = bool(triplets)
        mark_triple_extraction_result(
            [news_id] if has_triplets else [],
            [] if has_triplets else [news_id],
        )
    except Exception as e:
        logger.warning(
            "트리플 추출 실패 (is_processed=FALSE 유지, 다음 런 재시도): news_id=%s, %s: %s",
            news_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "has_triples" if has_triplets else "no_triples"


async def extract_unprocessed_triples(limit: int = DEFAULT_LIMIT) -> dict[str, int]:
    """
    미처리(material) 뉴스를 폴링해 트리플을 추출하고 Neo4j에 적재
    """

    items = fetch_unprocessed_triple_news_items(limit=limit)

    stats = {"fetched": len(items), "has_triples": 0, "no_triples": 0, "failed": 0}

    if not items:
        logger.info("트리플 추출 대상 뉴스가 없습니다.")
        return stats

    # 트리플관계 추출 LangGraph Runner 생성
    runner = GraphRunner()

    # 각 뉴스 하나씩 순차 실행
    async with neo4j_database:
        for item in items:
            status = await _process_item(runner, item)
            stats[status] += 1

    logger.info(
        "트리플 추출 완료: 조회 %d개, 관계있음 %d개, 관계없음 %d개, 실패 %d개",
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
