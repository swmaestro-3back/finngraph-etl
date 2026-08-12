from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database
from pipelines.news.loaders.news_repository import (
    fetch_unprocessed_triplet_news_items,
    insert_news_relations,
    mark_news_relation_extracted,
)
from pipelines.triplets.crud import build_news_relation_rows, upsert_triplets
from pipelines.triplets.graph.workflow import GraphRunner

logger = get_logger(__name__)

DEFAULT_LIMIT = 100


async def _process_item(runner: GraphRunner, item: dict[str, Any]) -> str:
    """
    1. LangGraph Runner 실행
    2. GraphDB에 삼중항관계 반영
    3. News 테이블에 relation_extracted 필드 최신화
    """
    news_id = item["news_id"]

    try:
        # 삼중항추출
        final_state = await runner.ainvoke(str(news_id), item["text"])
        triplets = final_state.get("triplets") or []

        # 삼중항관계가 존재한다면 Neo4j 그래프와 news_relations 테이블에 반영
        if triplets:
            await upsert_triplets(str(news_id), triplets)
            # 이항 엣지로 분해 + COMPANY ticker 조회 후 news_relations에 멱등 적재
            relation_rows = await build_news_relation_rows(triplets)
            insert_news_relations(news_id, relation_rows)

        has_triplets = bool(triplets)
        # News 테이블에 relation_extracted 최신화
        mark_news_relation_extracted(
            [news_id] if has_triplets else [],
            [] if has_triplets else [news_id],
        )
    except Exception as e:
        logger.warning(
            "삼중항 추출 실패 (NULL 유지, 다음 런 재시도): news_id=%s, %s: %s",
            news_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "has_triplets" if has_triplets else "no_triplets"


async def extract_unprocessed_triplets(limit: int = DEFAULT_LIMIT) -> dict[str, int]:
    """
    미처리(material) 뉴스를 폴링해 삼중항을 추출하고 Neo4j에 적재
    """

    items = fetch_unprocessed_triplet_news_items(limit=limit)

    stats = {"fetched": len(items), "has_triplets": 0, "no_triplets": 0, "failed": 0}

    if not items:
        logger.info("삼중항 추출 대상 뉴스가 없습니다.")
        return stats

    # 삼중항관계 추출 LangGraph Runner 생성
    runner = GraphRunner()

    # 각 뉴스 하나씩 순차 실행
    async with neo4j_database:
        for item in items:
            status = await _process_item(runner, item)
            stats[status] += 1

    logger.info(
        "삼중항 추출 완료: 조회 %d개, 관계있음 %d개, 관계없음 %d개, 실패 %d개",
        stats["fetched"],
        stats["has_triplets"],
        stats["no_triplets"],
        stats["failed"],
    )

    return stats


def run() -> dict[str, int]:
    return asyncio.run(extract_unprocessed_triplets())


if __name__ == "__main__":
    run()
