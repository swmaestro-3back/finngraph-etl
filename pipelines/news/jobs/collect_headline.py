from typing import Any, Dict, List, Tuple

from pipelines.news.config import (
    HEADLINE_MORE_COUNT,
    OFFICIAL_SOURCE_THRESHOLD,
    MAX_TOTAL_COLLECTED_ITEMS
)
from pipelines.news.extractors.anchor_headline_collector import (
    ANCHOR_CATEGORIES,
    iter_anchor_category_headline_pages,
    validate_anchor_headline_settings,
)
from pipelines.news.transformers.duplicate_filter import (
    normalize_title_for_duplicate,
    remove_duplicate_by_url,
    remove_duplicate_by_title,
)
from pipelines.news.loaders.news_repository import (
    filter_new_news_by_db,
    has_article_body,
    save_news_items
)
from pipelines.news.transformers.news_type_filter import filter_official_source_news
from pipelines.news.extractors.text_fetcher import enrich_items_with_article_body


def collect_category_new_headlines(
    category_id: int,
    target_count: int,
    max_more_calls: int = HEADLINE_MORE_COUNT,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:

    category_name = ANCHOR_CATEGORIES.get(category_id, str(category_id))

    collected: List[Dict[str, Any]] = []
    seen_titles: set = set()

    stats = {
        "category_name": category_name,
        "pages": 0,
        "raw": 0,
        "url_duplicate_removed": 0,
        "title_duplicate_removed": 0,
        "db_existing_removed": 0,
        "official_source_removed": 0,
        "selected": 0,
        "stopped_by_watermark": False,
        "stopped_by_target": False,
    }

    for page_items in iter_anchor_category_headline_pages(
        category_id=category_id,
        max_more_calls=max_more_calls,
    ):
        stats["pages"] += 1
        stats["raw"] += len(page_items)

        if not page_items:
            continue

        page_url_unique, page_url_removed = remove_duplicate_by_url(page_items)
        stats["url_duplicate_removed"] += len(page_url_removed)

        page_title_unique, page_title_removed = remove_duplicate_by_title(
            page_url_unique
        )
        stats["title_duplicate_removed"] += len(page_title_removed)

        # 카테고리 내 페이지 간 제목 중복 제거 (배치 헬퍼는 페이지 내부만 처리)
        page_candidates: List[Dict[str, Any]] = []
        for item in page_title_unique:
            normalized_title = normalize_title_for_duplicate(
                item.get("title", "")
            )

            if normalized_title and normalized_title in seen_titles:
                stats["title_duplicate_removed"] += 1
                continue

            if normalized_title:
                seen_titles.add(normalized_title)

            page_candidates.append(item)

        if not page_candidates:
            continue

        page_new_news, page_db_removed = filter_new_news_by_db(page_candidates)
        stats["db_existing_removed"] += len(page_db_removed)

        if not page_new_news:
            stats["stopped_by_watermark"] = True
            break

        page_official_news, page_official_removed = filter_official_source_news(
            items=page_new_news,
            pipeline_input={},
            official_source_threshold=OFFICIAL_SOURCE_THRESHOLD,
        )
        stats["official_source_removed"] += len(page_official_removed)

        collected.extend(page_official_news)

        if target_count and len(collected) >= target_count:
            collected = collected[:target_count]
            stats["stopped_by_target"] = True
            break

    stats["selected"] = len(collected)

    return collected, stats


def run() -> None:
    validate_anchor_headline_settings()

    category_ids = list(ANCHOR_CATEGORIES.keys())

    selected_news: List[Dict[str, Any]] = []
    category_result_counts: Dict[str, Dict[str, Any]] = {}

    for category_id in category_ids:
        category_selected_news, category_stats = collect_category_new_headlines(
            category_id=category_id,
            target_count=MAX_TOTAL_COLLECTED_ITEMS,
            max_more_calls=HEADLINE_MORE_COUNT,
        )

        selected_news.extend(category_selected_news)
        category_result_counts[category_stats["category_name"]] = category_stats

    selected_news = enrich_items_with_article_body(selected_news)

    storable_news = [
        item for item in selected_news
        if has_article_body(item)
    ]
    body_success_count = len(storable_news)
    body_failed_count = len(selected_news) - body_success_count

    save_result = save_news_items(
        items=storable_news,
        save_summary=False,
        skip_existing=True
    )

    print("\n" + "=" * 70)
    print("카테고리별 수집 결과")
    print("=" * 70)
    for category_name, category_stats in category_result_counts.items():
        stop_reason = (
            "목표달성" if category_stats["stopped_by_target"]
            else "워터마크(신규없음)" if category_stats["stopped_by_watermark"]
            else "페이지소진"
        )
        print(
            f"- {category_name}: 선정 {category_stats['selected']}개 "
            f"(페이지 {category_stats['pages']}, raw {category_stats['raw']}, "
            f"URL중복 {category_stats['url_duplicate_removed']}, "
            f"제목중복 {category_stats['title_duplicate_removed']}, "
            f"DB기존 {category_stats['db_existing_removed']}, "
            f"공식출처제외 {category_stats['official_source_removed']}, "
            f"중단사유 {stop_reason})"
        )

    print("\n" + "=" * 70)
    print(
        f"본문 추출 성공 {body_success_count}개 / 실패 {body_failed_count}개, "
        f"저장 결과 {save_result}"
    )
    print("작업 완료")
    print("=" * 70)


if __name__ == "__main__":
    run()
