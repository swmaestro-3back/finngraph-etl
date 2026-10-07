"""이슈 타임라인 백필: 기존 이슈(클러스터)에 타임라인 연결을 만든다.

--reset-links  임베딩과 연결 판정(parent_cluster_id, story_root_id, link_score, link_relation,
               linked_at)을 지운다. 잘못된 순서로 만든 연결을 고치거나, 임계값·임베딩 모델을 바꾼
               뒤 처음부터 다시 만들 때 쓴다. updated_at 은 건드리지 않는다.
--links        jobs/link_issues.link_pending 을 연결 대상이 없어질 때까지 반복한다(오래된 것부터).
               NEWS_ISSUE_LINK_ENABLED 와 무관하게 돈다.

둘 다 주면 초기화 → 연결 순서로 실행한다. 운영 반영 순서는 V12 적용 → 배포
(NEWS_ISSUE_LINK_ENABLED 꺼짐) → --links --apply 완료 → NEWS_ISSUE_LINK_ENABLED=true 이다. 이
설정을 먼저 켰으면 끄고 --reset-links --links --apply 로 다시 만든다. 스케줄 연결은 재판정 기간
(NEWS_ISSUE_RELINK_WINDOW_HOURS) 밖의 판정을 고치지 않는다. 그래서 옛 기사 백필
(news_backfill_krx100) 뒤에는 --reset-links --since-days <백필 기간> --links --apply 로,
재클러스터링(recluster_news.py --apply) 뒤에는 --links --apply 로 다시 만든다. 이 스크립트를
실행하는 동안에는 활성화 설정을 끈다.

같은 작업을 Airflow 수동 DAG news_backfill_issue_timeline 으로도 실행할 수 있다.

기본은 dry-run 이다. 쓰지도 Bedrock 을 부르지도 않고 대상 수와 예시만 출력한다(연결 판정은 앞선
판정의 기록을 부모 후보로 쓰므로 쓰지 않고는 미리 볼 수 없다). 실제로 쓰려면 --apply 를 준다.

실행:
    .venv/bin/python scripts/backfill_issue_timeline.py --links
    .venv/bin/python scripts/backfill_issue_timeline.py --links --limit 50 --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --reset-links --links --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --reset-links --since-days 30 --apply
"""

from __future__ import annotations

import argparse
import logging

from pipelines.news.jobs.backfill_issue_timeline import (
    backfill_links,
    reset_links,
    since_from_days,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("backfill_issue_timeline")

__all__ = ["backfill_links", "main", "reset_links"]


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
    if args.since_days is not None and args.since_days <= 0:
        parser.error("--since-days 는 양수")

    since = since_from_days(args.since_days)
    log.info(
        "[시작] %s, since=%s, limit=%s", "apply" if args.apply else "dry-run", since, args.limit
    )

    if args.reset_links:
        reset_links(since, args.apply)
    if args.links:
        backfill_links(since, args.limit, args.apply)


if __name__ == "__main__":
    main()
