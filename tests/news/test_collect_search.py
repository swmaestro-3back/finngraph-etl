"""collect_search 수집 모듈 단위 테스트 (API 불필요 — 전부 monkeypatch)."""

from __future__ import annotations

import pipelines.news.jobs.collect_search as collect_search
from pipelines.news.models import NewsArticle


def _raw(title: str, link: str, keyword: str) -> dict:
    return {
        "title": title,
        "description": "요약",
        "link": link,
        "originallink": link,
        "pubDate": "",
        "_search_keyword": keyword,
    }


def test_collect_search_news_returns_articles(monkeypatch):
    pages = {
        "유상증자": [[_raw("삼성전자 유상증자 결정", "https://n/1", "유상증자")]],
        "리콜": [[_raw("현대차 리콜 확대", "https://n/2", "리콜")]],
    }
    monkeypatch.setattr(
        collect_search, "iter_search_news_pages", lambda keyword: iter(pages[keyword])
    )

    articles, stats = collect_search.collect_search_news(["유상증자", "리콜"])

    assert all(isinstance(article, NewsArticle) for article in articles)
    assert all(article.source_type == "search" for article in articles)
    assert {article.search_keyword for article in articles} == {"유상증자", "리콜"}
    assert stats == {"queries": 2, "raw": 2, "duplicate_removed": 0, "collected": 2}


def test_collect_search_news_deduplicates_across_queries(monkeypatch):
    pages = {
        "유상증자": [[_raw("삼성전자 유상증자 결정", "https://n/1", "유상증자")]],
        "삼성전자": [[_raw("삼성전자 유상증자 결정", "https://n/1", "삼성전자")]],
    }
    monkeypatch.setattr(
        collect_search, "iter_search_news_pages", lambda keyword: iter(pages[keyword])
    )

    articles, stats = collect_search.collect_search_news(["유상증자", "삼성전자"])

    assert [article.title for article in articles] == ["삼성전자 유상증자 결정"]
    assert stats == {"queries": 2, "raw": 2, "duplicate_removed": 1, "collected": 1}
