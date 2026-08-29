"""키워드 검색 수집 모듈 (collect_news job이 사용)."""

from __future__ import annotations

from typing import Any

from pipelines.news.extractors.search_collector import (
    iter_search_news_pages,
)
from pipelines.news.models import NewsArticle, to_articles
from pipelines.news.transformers.duplicate_filter import (
    remove_duplicate_by_url,
)


def collect_search_news(queries: list[str]) -> tuple[list[NewsArticle], dict[str, int]]:
    """쿼리 목록으로 네이버 검색 API를 순회 수집하고 배치 내 중복을 제거한다."""

    collected: list[dict[str, Any]] = []
    raw_count = 0

    for query in queries:
        for page_items in iter_search_news_pages(keyword=query):
            raw_count += len(page_items)
            collected.extend(page_items)

    unique_items, url_removed = remove_duplicate_by_url(collected)

    articles = to_articles(unique_items, "search")

    stats = {
        "queries": len(queries),
        "raw": raw_count,
        "duplicate_removed": len(url_removed),
        "collected": len(articles),
    }

    return articles, stats
