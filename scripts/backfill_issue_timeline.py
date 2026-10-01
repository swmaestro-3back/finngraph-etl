"""이슈 타임라인 백필 — 기존 클러스터에 요약을 채우고 이야기 연결을 만든다.

--reset-links  임베딩과 연결 판정(parent_cluster_id, story_root_id, link_score, linked_at)을
               지운다. 잘못된 순서로 돈 연결을 처음부터 다시 만들 때 쓴다.
--summaries    이름은 있고 요약이 없는 클러스터에 요약만 채운다. 저장된 멤버 기사 본문 리드로
               운영과 같은 titler 를 부르되 기존 제목을 알려 주고, 응답의 새 제목은 버린다.
               이름과 updated_at 은 그대로 둔다.
--links        jobs/link_issues.link_pending 을 연결 대상이 없어질 때까지 반복한다(오래된 것부터).
               NEWS_ISSUE_LINK_ENABLED 와 무관하게 돈다.

여러 개를 주면 초기화 → 요약 → 연결 순서로 돈다. 한 번 저장된 임베딩·판정은 다시 만들지 않으므로
순서가 바뀌면 안 된다. 운영 반영 순서도 같다: V6 적용 → 배포(스위치 꺼짐) → --summaries --apply →
--links --apply 끝까지 → NEWS_ISSUE_LINK_ENABLED=true. 순서가 틀어졌으면 스위치를 끄고
--reset-links --summaries --links --apply 로 다시 만든다.

기본은 dry-run 이다. 쓰지도 Bedrock 을 부르지도 않고 대상 수와 예시만 출력한다(연결 판정은 앞선
판정의 기록에 기대므로 쓰지 않고는 미리 볼 수 없다). 실제로 쓰려면 --apply.

실행:
    .venv/bin/python scripts/backfill_issue_timeline.py --summaries --links
    .venv/bin/python scripts/backfill_issue_timeline.py --summaries --limit 50 --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --links --apply
    .venv/bin/python scripts/backfill_issue_timeline.py --reset-links --summaries --links --apply
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import KST, now_kst
from pipelines.news.config import get_news_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("langchain_aws").setLevel(logging.WARNING)
log = logging.getLogger("backfill_issue_timeline")

# --since-days 를 안 주면 전체 기간
ALL_TIME = datetime(1970, 1, 1, tzinfo=KST)
# 요약 백필은 이 단위로 LLM 을 부르고 바로 저장한다. 중간에 멈춰도 저장분은 남는다.
SUMMARY_CHUNK = 50
# dry-run 에서 보여줄 예시 수
SAMPLE_COUNT = 10
# limit 을 안 주면 사실상 전부
NO_LIMIT = 1_000_000


def reset_links(since: datetime, apply: bool) -> None:
    from pipelines.news.repositories import issue_timeline

    clusters, linked = issue_timeline.count_resettable_clusters(since)
    log.info("[초기화] 대상 %d개 (연결 판정 %d개)", clusters, linked)
    if not apply:
        return

    reset = issue_timeline.reset_links(since)
    log.info("[초기화] 임베딩·연결 판정 지움 %d개", reset)


def backfill_summaries(since: datetime, limit: int | None, apply: bool) -> None:
    from pipelines.news.repositories.news_clusters import (
        fetch_cluster_articles,
        fetch_unsummarized_clusters,
        update_cluster_summary,
    )
    from pipelines.news.transformers.cluster_titler import summarize_titled_clusters

    settings = get_news_settings()
    targets = fetch_unsummarized_clusters(since, limit or NO_LIMIT)
    log.info("[요약] 대상 %d개 (이름 있음 · 요약 없음)", len(targets))

    if not apply:
        for cluster_id, title in targets[:SAMPLE_COUNT]:
            log.info("  [dry-run] %s %s", cluster_id, title)
        return

    generated = saved = 0
    for chunk in chunked(targets, SUMMARY_CHUNK):
        titles = dict(chunk)
        articles = fetch_cluster_articles(list(titles))
        summaries = summarize_titled_clusters(
            {cid: (titles[cid], arts) for cid, arts in articles.items()},
            max_concurrency=settings.news_llm_max_concurrency,
            max_chars=settings.cluster_title_max_chars,
            summary_max_chars=settings.cluster_summary_max_chars,
        )
        generated += len(summaries)
        for cluster_id, summary in summaries.items():
            saved += update_cluster_summary(cluster_id, summary)
        log.info("[요약] 진행 %d / 생성 %d / 저장 %d", len(titles), generated, saved)

    log.info("[요약] 대상 %d → 생성 %d, 저장 %d", len(targets), generated, saved)


def backfill_links(since: datetime, limit: int | None, apply: bool) -> None:
    from pipelines.news.jobs import link_issues
    from pipelines.news.repositories.issue_timeline import count_link_targets, fetch_link_targets

    targets, without_embedding = count_link_targets(since)
    log.info("[연결] 대상 %d개 (임베딩 없음 %d개)", targets, without_embedding)

    if not apply:
        for target in fetch_link_targets(since, SAMPLE_COUNT):
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
    parser.add_argument("--summaries", action="store_true", help="요약이 없는 클러스터에 요약")
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
        help="요약·연결할 클러스터 수 상한 (--reset-links 는 기간 전체를 지운다)",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run", dest="apply", action="store_false", help="쓰지 않고 대상만 출력 (기본)"
    )
    mode.add_argument("--apply", dest="apply", action="store_true", help="실제로 쓴다")
    parser.set_defaults(apply=False)
    args = parser.parse_args(argv)

    if not (args.reset_links or args.summaries or args.links):
        parser.error("--reset-links, --summaries, --links 중 하나 이상을 준다")
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
    if args.summaries:
        backfill_summaries(since, args.limit, args.apply)
    if args.links:
        backfill_links(since, args.limit, args.apply)


if __name__ == "__main__":
    main()
