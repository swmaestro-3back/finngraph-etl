from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from news.config import (
    HEADLINE_MORE_COUNT,
    OFFICIAL_SOURCE_THRESHOLD,
    MAX_TOTAL_COLLECTED_ITEMS
)
from news.test.collectors.anchor_headline_collector import (
    ANCHOR_CATEGORIES,
    collect_anchor_category_headlines,
    validate_anchor_headline_settings,
)
from news.test.filters.duplicate_filter import remove_duplicate_by_url, remove_duplicate_by_title
from news.test.repositories.news_repository import (
    filter_new_news_by_db,
    has_article_body,
    save_news_items
)
from news.test.filters.news_type_filter import filter_official_source_news
from news.test.collectors.text_fetcher import enrich_items_with_article_body

def calculate_category_limit(
    total_limit: int,
    category_count: int
) -> int | None:
    """전체 수집 제한을 카테고리별 제한으로 변환한다."""

    if not total_limit:
        return None

    if category_count <= 0:
        raise ValueError("category_count는 1 이상이어야 합니다.")

    return max(total_limit // category_count, 1)


if __name__ == "__main__":
    validate_anchor_headline_settings()

    category_ids = list(ANCHOR_CATEGORIES.keys())
    category_limit = calculate_category_limit(
        total_limit=MAX_TOTAL_COLLECTED_ITEMS,
        category_count=len(category_ids)
    )

    raw_headline_news = []
    selected_news = []
    url_duplicate_removed_news = []
    title_duplicate_removed_news = []
    db_existing_removed_news = []
    title_keyword_removed_news = []
    category_result_counts = {}

    for category_id in category_ids:
        category_name = ANCHOR_CATEGORIES[category_id]

        category_raw_news = collect_anchor_category_headlines(
            category_id=category_id,
            more_click_count=HEADLINE_MORE_COUNT
        )

        raw_headline_news.extend(category_raw_news)

        category_url_unique_news, category_url_removed = remove_duplicate_by_url(
            category_raw_news
        )
        url_duplicate_removed_news.extend(category_url_removed)

        category_title_unique_news, category_title_removed = remove_duplicate_by_title(
            category_url_unique_news
        )
        title_duplicate_removed_news.extend(category_title_removed)

        category_new_news, category_db_removed = filter_new_news_by_db(
            category_title_unique_news
        )
        db_existing_removed_news.extend(category_db_removed)

        category_new_news, category_title_keyword_removed = filter_official_source_news(
            items=category_new_news,
            pipeline_input={},
            official_source_threshold=OFFICIAL_SOURCE_THRESHOLD
        )
        title_keyword_removed_news.extend(category_title_keyword_removed)

        if category_limit:
            category_selected_news = category_new_news[:category_limit]
        else:
            category_selected_news = category_new_news

        selected_news.extend(category_selected_news)

        category_result_counts[category_name] = {
            "raw": len(category_raw_news),
            "url_duplicate_removed": len(category_url_removed),
            "title_duplicate_removed": len(category_title_removed),
            "db_existing_removed": len(category_db_removed),
            "selected": len(category_selected_news)
        }

    selected_news = enrich_items_with_article_body(selected_news)

    body_success_count = len([
        item for item in selected_news
        if has_article_body(item)
    ])
    body_failed_count = len(selected_news) - body_success_count
    storable_news = [
        item for item in selected_news
        if has_article_body(item)
    ]

    save_result = save_news_items(
        items=storable_news,
        save_summary=False,
        skip_existing=True
    )

    print("\n" + "=" * 70)
    print("작업 완료")
    print("=" * 70)
