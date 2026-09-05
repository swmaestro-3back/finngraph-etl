"""기존 Event 노드 갱신 job (events_pipeline 의 sync_events task).

후보 클러스터 중 Neo4j 에 이미 Event 가 있는 것만 골라 카운터·시간 범위·대표·news_ids·
keywords 를 RDB 값으로 덮어쓰고, companies 로 간선을 다시 MERGE 한다. LLM 을 부르지 않고
title·companies 는 건드리지 않는다. RDB 에는 아무것도 쓰지 않는다.

generate_events 와 같은 DAG 런에서 병렬로 돈다. 둘 다 scan_promotable 로 자기 몫을 고르므로
task 사이에 XCom 이 없다. `scanned` 는 이 task 의 몫(기존 Event 가 있는 클러스터 수)이다.
갱신은 배치 하나라 실패하면 통째로 refresh_failed 로 센다.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.events.config import get_event_settings
from pipelines.events.loaders.neo4j import refresh_events
from pipelines.events.models import EventRefresh
from pipelines.events.references.rdb import fetch_cluster_news_ids
from pipelines.events.references.scan import scan_promotable
from pipelines.events.stats import check_total

logger = get_logger(__name__)

STAT_KEYS = ("scanned", "refreshed", "refresh_failed")


def summarize_stats(stats: dict[str, int]) -> dict[str, int]:
    return check_total(stats, "scanned", ("refreshed", "refresh_failed"))


async def _run() -> dict[str, int]:
    settings = get_event_settings()
    stats = dict.fromkeys(STAT_KEYS, 0)
    since = now_kst() - timedelta(days=settings.scan_days)

    async with neo4j_database:
        _, to_refresh = await scan_promotable(settings.min_size, since)
        stats["scanned"] = len(to_refresh)
        if not to_refresh:
            return stats

        news_ids = fetch_cluster_news_ids([c.cluster_id for c in to_refresh])
        refreshes = [
            EventRefresh(**c.model_dump(), news_ids=news_ids.get(c.cluster_id, []))
            for c in to_refresh
        ]
        try:
            stats["refreshed"] = await refresh_events(refreshes)
            stats["refresh_failed"] = len(refreshes) - stats["refreshed"]
        except Exception as e:
            logger.error(
                "Event 갱신 배치 실패: %d건, %s: %s",
                len(refreshes),
                type(e).__name__,
                e,
            )
            stats["refresh_failed"] = len(refreshes)

    return stats


def run() -> dict[str, int]:
    stats = summarize_stats(asyncio.run(_run()))

    print("\n" + "=" * 70)
    print("Event 갱신 결과 (sync_events)")
    print("=" * 70)
    print(
        f"- 기존 Event {stats['scanned']}개 중 갱신 {stats['refreshed']}개, "
        f"실패 {stats['refresh_failed']}개"
    )
    print("=" * 70)

    return stats


if __name__ == "__main__":
    run()
