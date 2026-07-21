import argparse

from pipelines.news.config import (
    MATERIAL_EVENT_FILTER_BATCH_SIZE,
    MATERIAL_EVENT_FILTER_MAX_CONCURRENCY,
    MATERIAL_EVENT_FILTER_MAX_WORKERS,
)
from pipelines.news.filters.material_event_filter import filter_material_event_news
from pipelines.news.repositories.news_repository import (
    delete_news_by_ids,
    extract_news_ids_from_removed_items,
    fetch_recent_news_items,
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
        description="필터링하고 삭제 후보 또는 실제 삭제를 수행"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="최근 뉴스 수",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 삭제한다. 없으면 dry-run으로 삭제 후보만 출력",
    )
    parser.add_argument(
        "--mode",
        choices=["sequential", "async", "thread", "batch"],
        default="sequential",
        help="LLM 필터 실행 방식. vLLM은 async 또는 thread, Ollama/cloud는 thread 사용",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MATERIAL_EVENT_FILTER_MAX_WORKERS,
        help="Ollama/cloud thread mode에서 동시에 처리할 요청 수",
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

    source_news = fetch_recent_news_items(limit=args.limit)

    passed_items, removed_items = filter_material_event_news(
        items=source_news,
        pipeline_input={},
        use_llm=True,
        mode=args.mode,
        max_workers=args.workers,
        max_concurrency=args.concurrency,
        batch_size=args.batch_size,
    )

    delete_candidate_ids = extract_news_ids_from_removed_items(removed_items)

    print("\n" + "=" * 80)
    print("의미 없는 뉴스 필터링 결과")
    print("=" * 80)
    print(f"조회 수: {len(source_news)}")
    print(f"통과 수: {len(passed_items)}")
    print(f"삭제 후보 수: {len(removed_items)}")
    print("=" * 80)

    if not args.apply:
        for removed in removed_items:
            print_removed_candidate(removed)

        return

    delete_result = delete_news_by_ids(delete_candidate_ids)

    print("\n" + "=" * 80)
    print("삭제 완료")
    print("=" * 80)
    print(f"삭제 요청 수: {delete_result.get('requested_count')}")
    print(f"삭제 임베딩 수: {delete_result.get('deleted_embedding_count')}")
    print(f"삭제 수: {delete_result.get('deleted_news_count')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
