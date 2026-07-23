from typing import Any

from pipelines.news.config import (
    KEYWORD_SEARCH_BATCH_SIZE,
    MAX_PAGES,
    SEARCH_DISPLAY,
    SEARCH_SORT,
)
from pipelines.news.extractors.search_collector import (
    SOURCE_TYPE_KEYWORD_SEARCH,
    iter_search_news_pages,
    validate_search_settings,
)
from pipelines.news.extractors.text_fetcher import enrich_items_with_article_body
from pipelines.news.loaders.news_repository import (
    filter_new_news_by_db,
    has_article_body,
    save_news_items,
)
from pipelines.news.loaders.search_keyword_repository import (
    fetch_active_search_keywords,
    mark_keywords_searched,
    mark_news_source_type,
    save_news_search_keyword_links,
    save_news_theme_links,
)
from pipelines.news.transformers.duplicate_filter import (
    normalize_title_for_duplicate,
    normalize_url_for_duplicate,
)


def _merge_keyword_ids(target: dict[str, Any], source: dict[str, Any]) -> None:
    existing_ids = target.setdefault("_search_keyword_ids", [])

    for keyword_id in source.get("_search_keyword_ids", []) or []:
        if keyword_id not in existing_ids:
            existing_ids.append(keyword_id)


def collect_news_for_keywords(
    keywords: list[dict[str, Any]],
    max_pages: int = MAX_PAGES,
    display: int = SEARCH_DISPLAY,
    sort: str = SEARCH_SORT,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:

    seen_by_url: dict[str, dict[str, Any]] = {}
    seen_by_title: dict[str, dict[str, Any]] = {}
    collected: list[dict[str, Any]] = []

    stats = {
        "keywords": len(keywords),
        "raw": 0,
        "duplicate_removed": 0,
        "collected": 0,
    }

    for keyword in keywords:
        keyword_id = keyword["id"]
        keyword_text = keyword["keyword"]

        for page_items in iter_search_news_pages(
            keyword=keyword_text,
            keyword_id=keyword_id,
            max_pages=max_pages,
            display=display,
            sort=sort,
        ):
            stats["raw"] += len(page_items)

            for item in page_items:
                normalized_link = normalize_url_for_duplicate(item.get("link", ""))
                normalized_originallink = normalize_url_for_duplicate(item.get("originallink", ""))
                normalized_title = normalize_title_for_duplicate(item.get("title", ""))

                matched = (
                    seen_by_url.get(normalized_link)
                    or seen_by_url.get(normalized_originallink)
                    or (seen_by_title.get(normalized_title) if normalized_title else None)
                )

                if matched is not None:
                    _merge_keyword_ids(matched, item)
                    stats["duplicate_removed"] += 1
                    continue

                for normalized_url in (normalized_link, normalized_originallink):
                    if normalized_url:
                        seen_by_url[normalized_url] = item

                if normalized_title:
                    seen_by_title[normalized_title] = item

                collected.append(item)

    stats["collected"] = len(collected)

    return collected, stats


def run() -> None:
    validate_search_settings()

    keywords = fetch_active_search_keywords(limit=KEYWORD_SEARCH_BATCH_SIZE)

    if not keywords:
        print("검색할 active 키워드가 없습니다.")
        return

    collected, collect_stats = collect_news_for_keywords(keywords)

    new_items, existing_items = filter_new_news_by_db(collected)
    new_items = enrich_items_with_article_body(new_items)

    storable_news = [item for item in new_items if has_article_body(item)]
    body_success_count = len(storable_news)
    body_failed_count = len(new_items) - body_success_count

    save_result = save_news_items(items=storable_news, save_summary=False, skip_existing=True)

    saved_news_ids = [item["_news_id"] for item in storable_news if item.get("_news_id")]
    mark_news_source_type(saved_news_ids, SOURCE_TYPE_KEYWORD_SEARCH)

    existing_matched_items = [existing["removed_item"] for existing in existing_items]
    provenance_items = storable_news + existing_matched_items
    link_result = save_news_search_keyword_links(provenance_items)

    keyword_theme_map = {
        keyword["id"]: keyword["source_id"]
        for keyword in keywords
        if keyword.get("source_type") in ("theme", "theme_stock") and keyword.get("source_id")
    }
    theme_link_result = save_news_theme_links(provenance_items, keyword_theme_map)

    searched_count = mark_keywords_searched([keyword["id"] for keyword in keywords])

    print("\n" + "=" * 70)
    print("키워드 검색 수집 결과")
    print("=" * 70)
    print(
        f"- 키워드 {collect_stats['keywords']}개 검색 "
        f"(raw {collect_stats['raw']}, 중복제거 {collect_stats['duplicate_removed']}, "
        f"수집 {collect_stats['collected']})"
    )
    print(
        f"- 신규 {len(new_items)}개 / 기존매칭 {len(existing_items)}개, "
        f"성공 {body_success_count} / 실패 {body_failed_count}"
    )
    print(f"- 저장 결과 {save_result}")
    print(
        f"- 근거 기록 {link_result}, 테마 연결 {theme_link_result}, 키워드 마킹 {searched_count}개"
    )
    print("작업 완료")
    print("=" * 70)


if __name__ == "__main__":
    run()
