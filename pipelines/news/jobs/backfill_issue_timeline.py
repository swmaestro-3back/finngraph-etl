"""이슈 타임라인 백필: 기존 이슈(클러스터)에 타임라인 연결을 만든다.

수동 DAG news_backfill_issue_timeline 이 이 모듈을 부른다. 스케줄
연결(jobs/link_issues.run)은 지금부터 lookback 안의 판정 전 이슈만 잇고, 이미 판정된 이슈는 재판정
기간 안에서만 다시 판정한다. 그래서 lookback 보다 오래된 이슈를 잇거나, 판정 기준을 바꾼 뒤 연결을
처음부터 다시 만들 때 이 백필을 쓴다.

apply 가 거짓이면 쓰지도 Bedrock 을 부르지도 않고 대상 수와 예시만 남긴다. 연결 판정은 앞선 판정을
부모 후보로 쓰므로 쓰지 않고는 결과를 미리 볼 수 없다.

판정 방식은 NEWS_ISSUE_LINK_METHOD 를 따른다. vote(기본)는 이슈마다 LLM 을 여러 번 부르므로(성격
분류, 제안자, 확인자) max_llm_calls 로 전체 새 호출 수를 제한할 수 있다. 배치마다 남은 호출 수를
실행당 상한으로 넘기므로 합계가 이 값을 넘지 않는다. 배치마다 실행당 상한
(NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN)도 따로 적용된다. 받은 응답은 캐시에 남으므로 멈췄다가 다시
실행해도 이어서 진행한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

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


def backfill_links(
    since: datetime, limit: int | None, apply: bool, max_llm_calls: int | None = None
) -> dict[str, Any]:
    """판정할 대상이 없어질 때까지 link_pending 을 오래된 것부터 반복하고 누적 통계를 돌려준다.

    NEWS_ISSUE_LINK_ENABLED 와 무관하게 돈다. 재판정 대상은 NEWS_ISSUE_RELINK_WINDOW_HOURS 가
    아니라 백필 기간 전체에서 고른다. max_llm_calls 를 주면 투표 판정의 LLM 새 호출 수 합계가 이
    값에 닿을 때 남은 대상을 두고 멈춘다.
    """

    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories.postgres import news_clusters

    settings = get_news_settings()
    summary_deadline = now_kst() - link_issues.SUMMARY_WAIT
    counts = news_clusters.count_link_targets(since, summary_deadline)
    logger.info(
        "[연결] 대상 %d개 (임베딩 없음 %d개), 대표 요약 대기로 빠짐 %d개, 판정 방식 %s",
        counts["targets"],
        counts["without_embedding"],
        counts["waiting_summary"],
        settings.issue_link_method,
    )

    totals: dict[str, Any] = dict.fromkeys(link_issues.STAT_KEYS, 0)
    if not apply:
        for target in news_clusters.fetch_link_targets(since, SAMPLE_COUNT, summary_deadline):
            logger.info(
                "  [dry-run] %s %s %s",
                target.cluster_id,
                target.first_published_at.astimezone(KST).date(),
                target.title,
            )
        return totals

    batch_size = settings.issue_link_max_per_run
    remaining = limit if limit is not None else NO_LIMIT
    while remaining > 0:
        # 재판정 대상은 백필 기간 전체에서 고른다. 실패하거나 미룬 옛 대상이 뒤 배치에서 판정되면,
        # 그보다 뒤에 시작해 앞 배치에서 판정된 이슈를 모두 다시 판정해야 시작 순서로 이은 결과와
        # 같아진다.
        stats = link_issues.link_pending(
            since=since,
            limit=min(batch_size, remaining),
            relink_since=since,
            llm_call_cap=None if max_llm_calls is None else max_llm_calls - totals["llm_calls"],
        )
        for key, value in stats.items():
            totals[key] += value
        remaining -= stats["scanned"]
        logger.info("[연결] 누적 %s", totals)
        # 실패하거나 미룬 대상은 linked_at 이 NULL 로 남아 다음 배치에 다시 잡힌다. 판정이 하나도
        # 없어도 실패 없이 새 호출을 했다면 받은 응답이 캐시에 남았으므로 다음 배치가 이어서
        # 진행한다. 그 밖에는 진전이 없으므로 멈춘다.
        progressed = stats["linked"] + stats["roots"] > 0 or (
            stats["llm_calls"] > 0 and stats["failed"] == 0
        )
        if stats["scanned"] == 0 or not progressed:
            break
        if max_llm_calls is not None and totals["llm_calls"] >= max_llm_calls:
            logger.info(
                "[연결] LLM 새 호출 %d회로 max_llm_calls 에 닿아 멈춘다", totals["llm_calls"]
            )
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
    max_llm_calls: int | None = None,
) -> dict[str, Any]:
    """초기화와 연결을 이 순서로 실행한다. 둘 다 거짓이면 ValueError 를 올린다."""

    if not (reset or links):
        raise ValueError("reset, links 중 하나 이상을 켠다")
    if limit is not None and limit <= 0:
        raise ValueError("limit 은 양수여야 한다")
    if max_llm_calls is not None and max_llm_calls <= 0:
        raise ValueError("max_llm_calls 는 양수여야 한다")

    since = since_from_days(since_days)
    logger.info(
        "[시작] %s, since=%s, limit=%s, max_llm_calls=%s",
        "apply" if apply else "dry-run",
        since,
        limit,
        max_llm_calls,
    )

    result: dict[str, Any] = {"reset": 0}
    if reset:
        result["reset"] = reset_links(since, apply)
    if links:
        result.update(backfill_links(since, limit, apply, max_llm_calls))
    return result
