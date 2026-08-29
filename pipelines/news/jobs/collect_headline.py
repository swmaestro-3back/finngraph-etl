"""카테고리 헤드라인 수집 모듈 (collect_news job이 사용)."""

from typing import Any

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.headline_collector import (
    iter_anchor_category_headline_pages,
)
from pipelines.news.loaders.postgres import (
    filter_new_news_by_db,
)
from pipelines.news.transformers.duplicate_filter import (
    normalize_title_for_duplicate,
    remove_duplicate_by_title,
    remove_duplicate_by_url,
)
from pipelines.news.transformers.news_type_filter import filter_official_source_news


def collect_category_new_headlines(
    category_id: int,
    target_count: int,
    max_more_calls: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:

    settings = get_news_settings()
    category_name = settings.anchor_categories.get(category_id, str(category_id))

    collected: list[dict[str, Any]] = []
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

        page_title_unique, page_title_removed = remove_duplicate_by_title(page_url_unique)
        stats["title_duplicate_removed"] += len(page_title_removed)

        # 카테고리 내 페이지 간 제목 중복 제거 (배치 헬퍼는 페이지 내부만 처리)
        page_candidates: list[dict[str, Any]] = []
        for item in page_title_unique:
            normalized_title = normalize_title_for_duplicate(item.get("title", ""))

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
            official_source_threshold=settings.official_source_threshold,
        )
        stats["official_source_removed"] += len(page_official_removed)

        collected.extend(page_official_news)

        if target_count and len(collected) >= target_count:
            collected = collected[:target_count]
            stats["stopped_by_target"] = True
            break

    stats["selected"] = len(collected)

    return collected, stats
