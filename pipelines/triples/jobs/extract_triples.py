from __future__ import annotations

import asyncio
from typing import Any

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.news.loaders.postgres import (
    fetch_unprocessed_triple_news_items,
    mark_triple_extraction_result,
)
from pipelines.triples.edges import source_row_of
from pipelines.triples.loaders.neo4j import sync_edge_summaries
from pipelines.triples.loaders.postgres import (
    insert_news_companies,
    insert_relation_sources,
)
from pipelines.triples.references.graph import fetch_company_tickers
from pipelines.triples.references.rdb import fetch_edge_summaries
from pipelines.triples.transformers.company_links import (
    collect_company_names,
    resolve_company_ids,
)
from pipelines.triples.workflow import GraphRunner

logger = get_logger(__name__)

DEFAULT_LIMIT = 200


async def _process_item(runner: GraphRunner, item: dict[str, Any]) -> str:
    """
    1. LangGraph Runner 실행
    2. relation_sources(근거 원장)에 뉴스 근거 적재 + news_companies(기업 매핑) 적재
    3. entities_relations 뷰 기준으로 Neo4j 간선 요약 동기화
    4. News 테이블에 triple_extracted 마킹
    """
    news_id = item["news_id"]

    try:
        # 트리플추출
        final_state = await runner.ainvoke(str(news_id), item["text"])
        triplets = final_state.get("triplets") or []

        if triplets:
            # 기업명→ticker 매핑은 원장 code 백필과 news_companies 해석에 공유한다.
            names = collect_company_names(triplets)
            name_to_ticker = await fetch_company_tickers(names)

            # 같은 뉴스 안에서 여러 문장이 같은 삼중항으로 수렴하면 첫 문장만 남긴다.
            source_rows: list[dict] = []
            seen_keys: set[tuple[str, str, str]] = set()
            for triplet in triplets:
                row = source_row_of(triplet, name_to_ticker)
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

            # 삼중항에 등장한 상장사를 news_companies에 연결
            insert_news_companies(news_id, resolve_company_ids(name_to_ticker))

        has_triplets = bool(triplets)
        mark_triple_extraction_result(
            [news_id] if has_triplets else [],
            [] if has_triplets else [news_id],
        )
    except Exception as e:
        logger.warning(
            "트리플 추출 실패 (triple_extracted=NULL 유지, 다음 런 재시도): news_id=%s, %s: %s",
            news_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "has_triples" if has_triplets else "no_triples"


async def extract_unprocessed_triples(limit: int = DEFAULT_LIMIT) -> dict[str, int]:
    """
    미처리(material) 뉴스를 폴링해 트리플을 추출하고 원장·그래프에 적재
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
