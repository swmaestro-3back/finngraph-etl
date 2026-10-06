"""
EVENT 노드 생성과 last_published_at 갱신
승격된 클러스터(대표와 제목이 있는 것) 중 Neo4j 에 없는 것을 EVENT 노드로 올린다.
제목은 cluster_articles 잡이 채운 news_clusters.title, 당사자는 클러스터 후보 기사 중
NEWS_EVENT_COMPANY_MIN_ARTICLES 건 이상의 news_companies 에 든 기업이다. LLM 을 부르지 않는다.

이미 노드가 있는 클러스터는 last_published_at 만 RDB 값으로 올린다. 승격 뒤 기사가 붙으면
assign 이 news_clusters.last_published_at 과 updated_at 을 갱신하므로, 같은 DAG 런의 이 잡이 스캔
범위 안에서 그 클러스터를 다시 본다.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.events.config import get_event_settings
from pipelines.events.models import ClusterCandidate, EventRecord
from pipelines.events.repositories.neo4j.events import create_event, update_last_published
from pipelines.events.repositories.postgres.news import fetch_cluster_company_ids
from pipelines.events.scan import scan_events
from pipelines.events.stats import check_total
from pipelines.news.config import get_news_settings

logger = get_logger(__name__)

# scanned·created·… 는 노드가 없는 클러스터,
# existing·updated·update_failed 는 노드가 있는 클러스터의 집계다.
STAT_KEYS = (
    "scanned",
    "created",
    "created_without_edges",
    "skipped_no_companies",
    "failed",
    "existing",
    "updated",
    "update_failed",
)


async def create_one(record: EventRecord) -> str:
    """노드와 간선을 쓴다. 실패하면 이 클러스터만 건너뛴다 — 노드가 없으니 다음 런에 재시도된다."""

    try:
        edges = await create_event(record)
    except Exception as e:
        logger.warning(
            "Event 생성 실패(건너뜀): cluster_id=%s, %s: %s",
            record.cluster_id,
            type(e).__name__,
            e,
        )
        return "failed"

    return "created" if edges > 0 else "created_without_edges"


async def refresh_existing(clusters: list[ClusterCandidate]) -> int | None:
    """기존 노드의 last_published_at 을 올린다. 실패하면 None — 다음 런의 스캔이 다시 잡는다."""

    try:
        return await update_last_published(clusters)
    except Exception as e:
        logger.warning(
            "Event last_published_at 갱신 실패(건너뜀): 대상 %d개, %s: %s",
            len(clusters),
            type(e).__name__,
            e,
        )
        return None


def summarize_stats(stats: dict[str, int]) -> dict[str, int]:
    """created_without_edges 는 created 의 부분집합이라 합에 없다. updated 는 existing 중 값이
    바뀐 노드 수라 합 검사 대상이 아니다."""

    return check_total(stats, "scanned", ("created", "skipped_no_companies", "failed"))


async def _run() -> dict[str, int]:
    settings = get_event_settings()
    promote_size = get_news_settings().cluster_promote_size
    stats = dict.fromkeys(STAT_KEYS, 0)
    since = now_kst() - timedelta(days=settings.scan_days)

    async with neo4j_database:
        # 1. 후보 스캔 — 승격되고 제목이 붙은 클러스터를 Event 노드 유무로 나눈다
        clusters, existing = await scan_events(promote_size, since)
        stats["scanned"] = len(clusters)
        stats["existing"] = len(existing)

        # 2. 기존 노드 — last_published_at 만 올린다
        if existing:
            updated = await refresh_existing(existing)
            if updated is None:
                stats["update_failed"] = len(existing)
            else:
                stats["updated"] = updated

        if not clusters:
            return stats

        # 3. 당사자 — 후보 기사 여러 건에 걸쳐 연결된 기업 (기업 0 은 노드를 만들지 않는다)
        company_ids = fetch_cluster_company_ids(
            [cluster.cluster_id for cluster in clusters], settings.company_min_articles
        )

        # 4. 쓰기 (클러스터별 격리)
        for cluster in clusters:
            ids = company_ids.get(cluster.cluster_id, [])
            if not ids:
                stats["skipped_no_companies"] += 1
                continue

            outcome = await create_one(EventRecord(**cluster.model_dump(), company_ids=ids))
            if outcome == "failed":
                stats["failed"] += 1
                continue
            stats["created"] += 1
            if outcome == "created_without_edges":
                stats["created_without_edges"] += 1

    return stats


def run() -> dict[str, int]:
    stats = summarize_stats(asyncio.run(_run()))

    print("\n" + "=" * 70)
    print("Event 생성 결과 (generate_events)")
    print("=" * 70)
    print(f"- Event 없는 승격 클러스터 {stats['scanned']}개")
    print(
        f"- 생성 {stats['created']}개 (간선 없음 {stats['created_without_edges']}개), "
        f"실패 {stats['failed']}개"
    )
    print(f"- 건너뜀: 당사자 기업 없음 {stats['skipped_no_companies']}개")
    print(
        f"- 기존 Event {stats['existing']}개 중 last_published_at 갱신 {stats['updated']}개"
        + (f", 갱신 실패 {stats['update_failed']}개" if stats["update_failed"] else "")
    )
    print("=" * 70)

    return stats


if __name__ == "__main__":
    run()
