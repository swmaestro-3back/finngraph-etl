"""이슈 타임라인 백필: 기존 이슈(클러스터)에 타임라인 연결을 만든다.

news_backfill_issue_timeline DAG 와 scripts/backfill_issue_timeline.py 가 이 모듈을 부른다. 스케줄
연결(jobs/link_issues.run)은 지금부터 lookback 안의 판정 전 이슈만 잇고, 이미 판정된 이슈는 재판정
기간 안에서만 다시 판정한다. 그래서 lookback 보다 오래된 이슈를 잇거나, 판정 기준을 바꾼 뒤 연결을
처음부터 다시 만들 때 이 백필을 쓴다.

apply 가 거짓이면 쓰지도 Bedrock 을 부르지도 않고 대상 수와 예시만 남긴다. 연결 판정은 앞선 판정을
부모 후보로 쓰므로 쓰지 않고는 결과를 미리 볼 수 없다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.common.logging import get_logger
from pipelines.common.utils.time import KST, now_kst
from pipelines.news.config import get_news_settings

logger = get_logger(__name__)

# since 를 주지 않으면 전체 기간을 대상으로 한다.
ALL_TIME = datetime(1970, 1, 1, tzinfo=KST)
# dry-run 에서 남길 예시 수다.
SAMPLE_COUNT = 10
# limit 을 주지 않으면 사실상 전부 잇는다.
NO_LIMIT = 1_000_000


def since_from_days(since_days: int | None) -> datetime:
    """지금부터 since_days 일 전 시각을 돌려준다. None 이면 전체 기간이다."""

    if since_days is None:
        return ALL_TIME
    if since_days <= 0:
        # 0 을 전체 기간으로 처리하면 초기화가 의도보다 넓게 지운다.
        raise ValueError("since_days 는 양수여야 한다")
    return now_kst() - timedelta(days=since_days)


def reset_links(since: datetime, apply: bool) -> int:
    """since 이후에 시작한 클러스터의 임베딩과 연결 판정을 지우고 지운 수를 돌려준다."""

    from pipelines.news.repositories.postgres import news_clusters

    counts = news_clusters.count_resettable_clusters(since)
    logger.info("[초기화] 대상 %d개 (연결 판정 %d개)", counts["clusters"], counts["linked"])
    if not apply:
        return 0

    reset = news_clusters.reset_cluster_links(since)
    logger.info("[초기화] 임베딩·연결 판정 지움 %d개", reset)
    return reset


def backfill_links(since: datetime, limit: int | None, apply: bool) -> dict[str, int]:
    """판정할 대상이 없어질 때까지 link_pending 을 오래된 것부터 반복하고 누적 통계를 돌려준다.

    NEWS_ISSUE_LINK_ENABLED 와 무관하게 돈다.
    """

    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories.postgres import news_clusters

    summary_deadline = now_kst() - link_issues.SUMMARY_WAIT
    counts = news_clusters.count_link_targets(since, summary_deadline)
    logger.info(
        "[연결] 대상 %d개 (임베딩 없음 %d개), 대표 요약 대기로 빠짐 %d개",
        counts["targets"],
        counts["without_embedding"],
        counts["waiting_summary"],
    )

    totals = dict.fromkeys(link_issues.STAT_KEYS, 0)
    if not apply:
        for target in news_clusters.fetch_link_targets(since, SAMPLE_COUNT, summary_deadline):
            logger.info(
                "  [dry-run] %s %s %s",
                target.cluster_id,
                target.first_published_at.astimezone(KST).date(),
                target.title,
            )
        return totals

    batch_size = get_news_settings().issue_link_max_per_run
    remaining = limit if limit is not None else NO_LIMIT
    while remaining > 0:
        stats = link_issues.link_pending(since=since, limit=min(batch_size, remaining))
        for key, value in stats.items():
            totals[key] += value
        remaining -= stats["scanned"]
        logger.info("[연결] 누적 %s", totals)
        # 실패한 대상은 linked_at 이 NULL 로 남아 다음 배치에 다시 잡히므로, 하나도 판정하지 못한
        # 배치가 나오면 멈춘다.
        if stats["scanned"] == 0 or stats["linked"] + stats["roots"] == 0:
            break

    logger.info("[연결] 합계 %s", totals)
    return totals


def run(
    *,
    reset: bool,
    links: bool,
    since_days: int | None,
    limit: int | None,
    apply: bool,
) -> dict[str, int]:
    """초기화와 연결을 이 순서로 실행한다. 둘 다 거짓이면 ValueError 를 올린다."""

    if not (reset or links):
        raise ValueError("reset, links 중 하나 이상을 켠다")
    if limit is not None and limit <= 0:
        raise ValueError("limit 은 양수여야 한다")

    since = since_from_days(since_days)
    logger.info("[시작] %s, since=%s, limit=%s", "apply" if apply else "dry-run", since, limit)

    result: dict[str, int] = {"reset": 0}
    if reset:
        result["reset"] = reset_links(since, apply)
    if links:
        result.update(backfill_links(since, limit, apply))
    return result
