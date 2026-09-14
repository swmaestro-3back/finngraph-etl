"""저장된 news 만으로 클러스터를 다시 판정한다 (일회성 백필).

1. Neo4j Event 노드·HAS_EVENT 간선 삭제, news_clusters 전부 삭제, news.cluster_id 초기화
2. news 를 발행일(KST) 순으로 하루씩 묶어 운영과 같은 규칙(창·cap·시드 합류)으로 다시 판정
3. original_size 가 기준을 넘은 클러스터에 LLM 으로 이름 생성
4. events.generate_events 로 Neo4j Event 승격 (상한 단위로 반복)

실행: .venv/bin/python scripts/recluster_news.py
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime

from sqlalchemy import text

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.clients.postgres import session_scope
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.news_clusters import (
    fetch_active_cluster_seeds,
    fetch_cluster_articles,
    fetch_untitled_cluster_ids,
    record_cluster_assignments,
    update_cluster_title,
)
from pipelines.news.transformers.cluster_titler import title_clusters
from pipelines.news.transformers.clustering import assign_batch, document_terms, seed_window
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("recluster")


def reset() -> None:
    async def wipe_graph() -> None:
        async with neo4j_database:
            [row] = await neo4j_database.execute(
                "MATCH (e:Event) OPTIONAL MATCH ()-[h:HAS_EVENT]->(e) "
                "RETURN count(DISTINCT e) AS events, count(h) AS edges"
            )
            await neo4j_database.execute("MATCH (e:Event) DETACH DELETE e")
            log.info("[초기화] Neo4j Event %d개, HAS_EVENT %d개 삭제", row["events"], row["edges"])

    asyncio.run(wipe_graph())

    with session_scope() as session:
        session.execute(text("UPDATE news SET cluster_id = NULL, cluster_terms = NULL"))
        deleted = session.execute(text("DELETE FROM news_clusters")).rowcount
    log.info("[초기화] news_clusters %d행 삭제, news.cluster_id 초기화", deleted)


def load_news_by_day() -> dict[object, list[dict]]:
    """발행일(KST) → 기사 목록. 발행 시각이 없으면 수집 시각을 쓴다."""

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT id, title, COALESCE(published_at, collected_at) AS published_at
                  FROM news
                 ORDER BY 3, id;
                """
            )
        ).fetchall()

    by_day: dict[object, list[dict]] = defaultdict(list)
    for news_id, title, published_at in rows:
        published_at = published_at.astimezone(SEOUL_TIMEZONE)
        by_day[published_at.date()].append(
            {
                "_news_id": int(news_id),
                "_save_action": "inserted",
                "title": title or "",
                "published_at": published_at,
            }
        )
    return by_day


def recluster() -> dict[str, int]:
    settings = get_news_settings()
    totals = {"days": 0, "news": 0, "created": 0, "updated": 0, "failed": 0, "dropped": 0}

    for day, items in sorted(load_news_by_day().items()):
        documents = [
            document_terms(item["title"], "", settings.cluster_description_weight) for item in items
        ]
        published_ats = [item["published_at"] for item in items]

        window_start, window_end = seed_window(published_ats, settings.cluster_window_days)
        seeds = fetch_active_cluster_seeds(window_start, window_end)
        assignments = assign_batch(
            documents,
            published_ats,
            seeds,
            threshold=settings.cluster_threshold,
            cap=settings.cluster_max_articles,
            window_days=settings.cluster_window_days,
        )
        result = record_cluster_assignments(
            assignments, items, documents, settings.cluster_keyword_count
        )

        totals["days"] += 1
        totals["news"] += len(items)
        for key in ("created", "updated", "failed"):
            totals[key] += result[key]
        totals["dropped"] += sum(len(a.dropped) for a in assignments)
        log.info(
            "[재판정] %s 기사 %d건 → 시드 %d, 생성 %d, 갱신 %d, cap 제외 %d",
            day,
            len(items),
            len(seeds),
            result["created"],
            result["updated"],
            sum(len(a.dropped) for a in assignments),
        )

    return totals


def title_all(since: datetime) -> tuple[int, int]:
    settings = get_news_settings()
    untitled = fetch_untitled_cluster_ids(settings.cluster_title_min_size, since)
    titles = title_clusters(
        fetch_cluster_articles(untitled),
        max_concurrency=settings.news_llm_max_concurrency,
        max_chars=settings.cluster_title_max_chars,
    )
    for cluster_id, title in titles.items():
        update_cluster_title(cluster_id, title)
    log.info(
        "[제목] 대상 %d → 생성 %d / 실패 %d",
        len(untitled),
        len(titles),
        len(untitled) - len(titles),
    )
    return len(untitled), len(titles)


def promote() -> dict[str, int]:
    from pipelines.events.jobs import generate_events

    totals: dict[str, int] = defaultdict(int)
    for _ in range(50):  # max_items_per_run 단위로 반복
        stats = generate_events.run()
        for key, value in stats.items():
            totals[key] += value
        if stats["created"] + stats["failed"] == 0 or stats["skipped_over_limit"] == 0:
            break
    return dict(totals)


def main() -> None:
    started_at = datetime.now(SEOUL_TIMEZONE)
    reset()
    totals = recluster()
    log.info("[재판정 합계] %s", totals)
    title_all(started_at)
    log.info("[승격 합계] %s", promote())


if __name__ == "__main__":
    main()
