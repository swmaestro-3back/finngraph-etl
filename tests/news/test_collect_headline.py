"""collect_headline 수집 모듈 단위 테스트 (크롤링/DB 불필요 — 전부 monkeypatch)."""

from __future__ import annotations

from types import SimpleNamespace

import pipelines.news.jobs.collect_headline as collect_headline
from pipelines.news.models import NewsArticle


def _page_item(title: str, link: str, category_id: int = 101, category_name: str = "경제") -> dict:
    return {
        "title": title,
        "description": "",
        "link": link,
        "originallink": link,
        "pubDate": "",
        "_category_id": category_id,
        "_category_name": category_name,
    }


def _stub_settings() -> SimpleNamespace:
    return SimpleNamespace(
        anchor_categories={101: "경제"},
        official_source_threshold=0,
    )


def test_collect_category_new_headlines_returns_articles(monkeypatch):
    monkeypatch.setattr(collect_headline, "get_news_settings", _stub_settings)
    monkeypatch.setattr(collect_headline, "filter_new_news_by_db", lambda items: (items, []))
    monkeypatch.setattr(
        collect_headline,
        "iter_anchor_category_headline_pages",
        lambda category_id, max_more_calls: iter(
            [
                [
                    _page_item("삼성전자 유상증자 결정", "https://n/1"),
                    _page_item("현대차 리콜 확대", "https://n/2"),
                ]
            ]
        ),
    )

    articles, stats = collect_headline.collect_category_new_headlines(
        category_id=101, target_count=10
    )

    assert all(isinstance(article, NewsArticle) for article in articles)
    assert [article.title for article in articles] == ["삼성전자 유상증자 결정", "현대차 리콜 확대"]
    assert all(article.source_type == "headline" for article in articles)
    assert all(article.category_id == 101 for article in articles)
    assert all(article.category_name == "경제" for article in articles)
    assert stats["selected"] == 2


def test_collect_category_new_headlines_skips_contract_violations(monkeypatch):
    monkeypatch.setattr(collect_headline, "get_news_settings", _stub_settings)
    monkeypatch.setattr(collect_headline, "filter_new_news_by_db", lambda items: (items, []))
    monkeypatch.setattr(
        collect_headline,
        "iter_anchor_category_headline_pages",
        lambda category_id, max_more_calls: iter(
            [
                [
                    _page_item("정상 기사", "https://n/1"),
                    _page_item("링크 없는 기사", ""),  # link 누락 → 계약 위반
                ]
            ]
        ),
    )

    articles, stats = collect_headline.collect_category_new_headlines(
        category_id=101, target_count=10
    )

    assert [article.title for article in articles] == ["정상 기사"]
    assert stats["selected"] == 1
