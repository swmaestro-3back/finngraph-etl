from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.news.loaders.news_repository import (
    fetch_unprocessed_triple_news_items,
    insert_news_relations,
    mark_news_relation_extracted,
)
from pipelines.triples.extractors.workflow import GraphRunner
from pipelines.triples.loaders.neo4j import upsert_triples
from pipelines.triples.transformers.news_relations import build_news_relation_rows

logger = get_logger(__name__)

DEFAULT_LIMIT = 100


async def _process_item(runner: GraphRunner, item: dict[str, Any]) -> str:
    """
    1. LangGraph Runner 실행
    2. GraphDB에 트리플관계 반영
    3. News 테이블에 relation_extracted 필드 최신화
    """
    news_id = item["news_id"]

    try:
        # 트리플추출
        final_state = await runner.ainvoke(str(news_id), item["text"])
        triples = final_state.get("triples") or []

        # 트리플관계가 존재한다면 Neo4j 그래프와 news_relations 테이블에 반영
        if triples:
            await upsert_triples(str(news_id), triples)
            # 이항 엣지로 분해 + COMPANY ticker 조회 후 news_relations에 멱등 적재
            relation_rows = await build_news_relation_rows(triples)
            insert_news_relations(news_id, relation_rows)

        has_triples = bool(triples)
        # News 테이블에 relation_extracted 최신화
        mark_news_relation_extracted(
            [news_id] if has_triples else [],
            [] if has_triples else [news_id],
        )
    except Exception as e:
        logger.warning(
            "트리플 추출 실패 (NULL 유지, 다음 런 재시도): news_id=%s, %s: %s",
            news_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "has_triples" if has_triples else "no_triples"


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
