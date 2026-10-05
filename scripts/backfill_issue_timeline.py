"""이슈 타임라인 백필 — 기존 이슈(클러스터)에 이야기 연결을 만든다.

--reset-links  임베딩과 연결 판정(parent_cluster_id, story_root_id, link_score, link_relation,
               linked_at)을 지운다. 잘못된 순서로 돈 연결이나 임계값·임베딩 모델을 바꾼 뒤 처음부터
               다시 만들 때 쓴다. updated_at 은 건드리지 않는다.
--links        jobs/link_issues.link_pending 을 연결 대상이 없어질 때까지 반복한다(오래된 것부터).
               NEWS_ISSUE_LINK_ENABLED 와 무관하게 돈다.

둘 다 주면 초기화 → 연결 순서로 돈다. 운영 반영 순서: V12 적용 → 배포(스위치 꺼짐) →
--links --apply 끝까지 → NEWS_ISSUE_LINK_ENABLED=true. 스위치를 먼저 켰으면 끄고
--reset-links --links --apply 로 다시 만든다. 스케줄 연결은 꼬리 재판정 창
(NEWS_ISSUE_RELINK_WINDOW_HOURS) 밖의 판정을 고치지 않으므로, 옛 기사 백필(news_backfill_krx100)
뒤에는 --reset-links --since-days <백필 기간> --links --apply, 클러스터 재판정
(recluster_news.py --apply) 뒤에는 --links --apply 로 다시 만든다. 돌리는 동안에는 스위치를 끈다.

기본은 dry-run 이다. 쓰지도 Bedrock 을 부르지도 않고 대상 수와 예시만 출력한다(연결 판정은 앞선
판정의 기록에 기대므로 쓰지 않고는 미리 볼 수 없다). 실제로 쓰려면 --apply.

실행:
    .venv/bin/python scripts/backfill_issue_timeline.py --links
    .venv/bin/python scripts/backfill_issue_timeline.py --links --limit 50 --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --reset-links --links --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --reset-links --since-days 30 --apply
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

from pipelines.common.utils.time import KST, now_kst
from pipelines.news.config import get_news_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("backfill_issue_timeline")

# --since-days 를 안 주면 전체 기간
ALL_TIME = datetime(1970, 1, 1, tzinfo=KST)
# dry-run 에서 보여줄 예시 수
SAMPLE_COUNT = 10
# limit 을 안 주면 사실상 전부
NO_LIMIT = 1_000_000


def reset_links(since: datetime, apply: bool) -> None:
    from pipelines.news.repositories.postgres import news_clusters

    counts = news_clusters.count_resettable_clusters(since)
    log.info("[초기화] 대상 %d개 (연결 판정 %d개)", counts["clusters"], counts["linked"])
    if not apply:
        return

    reset = news_clusters.reset_cluster_links(since)
    log.info("[초기화] 임베딩·연결 판정 지움 %d개", reset)


def backfill_links(since: datetime, limit: int | None, apply: bool) -> None:
    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories.postgres import news_clusters

    summary_deadline = now_kst() - link_issues.SUMMARY_WAIT
    counts = news_clusters.count_link_targets(since, summary_deadline)
    log.info(
        "[연결] 대상 %d개 (임베딩 없음 %d개), 대표 요약 대기로 빠짐 %d개",
        counts["targets"],
        counts["without_embedding"],
        counts["waiting_summary"],
    )

    if not apply:
        for target in news_clusters.fetch_link_targets(since, SAMPLE_COUNT, summary_deadline):
            log.info(
                "  [dry-run] %s %s %s",
                target.cluster_id,
                target.first_published_at.astimezone(KST).date(),
                target.title,
            )
        return

    batch_size = get_news_settings().issue_link_max_per_run
    totals = dict.fromkeys(link_issues.STAT_KEYS, 0)
    remaining = limit if limit is not None else NO_LIMIT
    while remaining > 0:
        stats = link_issues.link_pending(since=since, limit=min(batch_size, remaining))
        for key, value in stats.items():
            totals[key] += value
        remaining -= stats["scanned"]
        log.info("[연결] 누적 %s", totals)
        # 실패한 대상은 linked_at 이 NULL 로 남아 다시 잡힌다. 진전이 없으면 멈춘다.
        if stats["scanned"] == 0 or stats["linked"] + stats["roots"] == 0:
            break

    log.info("[연결] 합계 %s", totals)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--reset-links", action="store_true", help="임베딩·연결 판정을 지우고 처음부터"
    )
    parser.add_argument("--links", action="store_true", help="연결 대상이 없어질 때까지 연결")
    parser.add_argument(
        "--since-days",
        type=int,
        default=None,
        help="first_published_at 이 지금부터 이 일수 안인 클러스터만 (기본: 전체)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="연결할 클러스터 수 상한 (--reset-links 는 기간 전체를 지운다)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", dest="apply", action="store_false", help="쓰지 않고 대상만 출력 (기본)"
    )
    mode.add_argument("--apply", dest="apply", action="store_true", help="실제로 쓴다")
    parser.set_defaults(apply=False)
    args = parser.parse_args(argv)

    if not (args.reset_links or args.links):
        parser.error("--reset-links, --links 중 하나 이상을 준다")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit 은 양수")
    # 0 이 '전체'로 읽히면 --reset-links 가 의도보다 넓게 지운다
    if args.since_days is not None and args.since_days <= 0:
        parser.error("--since-days 는 양수")

    since = now_kst() - timedelta(days=args.since_days) if args.since_days else ALL_TIME
    log.info(
        "[시작] %s, since=%s, limit=%s", "apply" if args.apply else "dry-run", since, args.limit
    )

    if args.reset_links:
        reset_links(since, args.apply)
    if args.links:
        backfill_links(since, args.limit, args.apply)


if __name__ == "__main__":
    main()
