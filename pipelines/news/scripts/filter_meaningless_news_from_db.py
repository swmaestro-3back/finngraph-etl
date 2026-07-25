import argparse

from pipelines.news.config import (
    MATERIAL_EVENT_FILTER_BATCH_SIZE,
    MATERIAL_EVENT_FILTER_MAX_CONCURRENCY,
    MATERIAL_EVENT_FILTER_MAX_WORKERS,
)
from pipelines.news.jobs.filter_meaningless_news import (
    DEFAULT_MAX_ITEMS_PER_RUN,
    run,
)
from pipelines.news.utils.text_utils import get_printable_text


def print_removed_candidate(removed):
    item = removed.get("removed_item", {})
    analysis = removed.get("debug_info", {}).get("material_event", {})
    news_id = item.get("_news_id", "-")
    title = get_printable_text(item.get("title", ""))
    provider = analysis.get("provider", "unknown")

    print("-" * 80)
    print(f"삭제 후보 id={news_id} provider={provider}")
    print(f"제목: {title}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "material_event 필터로 판정한다. 기본은 dry-run이며 "
            "--apply 시 소프트 삭제(is_material=false)로 기록한다."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_MAX_ITEMS_PER_RUN,
        help="한 번에 처리할 미판정 뉴스 상한",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 소프트 삭제 기록. 없으면 dry-run으로 삭제 후보만 출력",
    )
    parser.add_argument(
        "--mode",
        choices=["sequential", "async", "thread", "batch"],
        default="sequential",
        help="LLM 필터 실행 방식. vLLM은 async 또는 thread, Ollama는 thread 사용",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MATERIAL_EVENT_FILTER_MAX_WORKERS,
        help="Ollama thread mode에서 동시에 처리할 요청 수",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=MATERIAL_EVENT_FILTER_MAX_CONCURRENCY,
        help="vLLM async mode에서 동시에 처리할 요청 수",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MATERIAL_EVENT_FILTER_BATCH_SIZE,
        help="batch mode에서 요청 1개에 넣을 데이터 수",
    )
    args = parser.parse_args()

    summary = run(
        apply=args.apply,
        limit=args.limit,
        mode=args.mode,
        max_workers=args.workers,
        max_concurrency=args.concurrency,
        batch_size=args.batch_size,
    )

    if not args.apply:
        for removed in summary.get("removed_items", []):
            print_removed_candidate(removed)


if __name__ == "__main__":
    main()
