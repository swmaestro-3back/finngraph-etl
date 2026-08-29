"""collect_news job 단위 테스트 (API/DB 불필요 — 전부 monkeypatch)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import pipelines.news.jobs.collect_news as collect_news


def _item(title: str, description: str = "", link: str | None = None) -> dict:
    return {
        "title": title,
        "description": description,
        "link": link or f"https://n.news.naver.com/{title}",
        "originallink": "",
        "pubDate": "",
    }


def _stub_settings() -> SimpleNamespace:
    return SimpleNamespace(
        anchor_categories={101: "경제", 103: "증권"},
        max_total_collected_items=100,
        headline_more_count=3,
        official_source_threshold=0,
        cluster_threshold=0.35,
        cluster_description_weight=0.4,
        cluster_max_articles=3,
    )


def test_collect_headline_items_tags_source_type(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_anchor_headline_settings", lambda: None)
    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)

    def fake_collect(category_id, target_count, max_more_calls):
        return [_item(f"카테고리{category_id} 기사", link=f"https://n/{category_id}")], {}

    monkeypatch.setattr(collect_news, "collect_category_new_headlines", fake_collect)

    items = collect_news.collect_headline_items()

    assert len(items) == 2  # 카테고리 2개 × 1건
    assert all(item["_source_type"] == "headline" for item in items)


def test_collect_search_items_tags_source_and_returns_keyword_ids(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_search_settings", lambda: None)
    monkeypatch.setattr(
        collect_news,
        "fetch_search_keywords",
        lambda: [{"id": 7, "keyword": "유상증자"}, {"id": 9, "keyword": "리콜"}],
    )
    monkeypatch.setattr(
        collect_news,
        "collect_search_news",
        lambda queries: ([_item(f"{q} 기사") for q in queries], {}),
    )

    items, keyword_ids = collect_news.collect_search_items()

    assert keyword_ids == [7, 9]
    assert all(item["_source_type"] == "search" for item in items)


def test_collect_search_items_empty_keywords(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_search_settings", lambda: None)
    monkeypatch.setattr(collect_news, "fetch_search_keywords", lambda: [])

    items, keyword_ids = collect_news.collect_search_items()

    assert items == []
    assert keyword_ids == []


def test_collect_all_sources_merges_headline_first(monkeypatch):
    headline = [_item("헤드라인 기사")]
    search = [_item("검색 기사")]
    monkeypatch.setattr(collect_news, "collect_headline_items", lambda: headline)
    monkeypatch.setattr(collect_news, "collect_search_items", lambda: (search, [7]))

    merged, keyword_ids, stats = collect_news.collect_all_sources()

    assert merged == headline + search
    assert keyword_ids == [7]
    assert stats == {"headline_collected": 1, "search_collected": 1, "failed_sources": []}


def test_collect_all_sources_continues_when_headline_fails(monkeypatch):
    def boom():
        raise RuntimeError("crawl down")

    monkeypatch.setattr(collect_news, "collect_headline_items", boom)
    monkeypatch.setattr(collect_news, "collect_search_items", lambda: ([_item("검색 기사")], [7]))

    merged, keyword_ids, stats = collect_news.collect_all_sources()

    assert len(merged) == 1
    assert keyword_ids == [7]
    assert stats["failed_sources"] == ["headline"]


def test_collect_all_sources_skips_keywords_when_search_fails(monkeypatch):
    def boom():
        raise RuntimeError("api down")

    monkeypatch.setattr(collect_news, "collect_headline_items", lambda: [_item("헤드라인 기사")])
    monkeypatch.setattr(collect_news, "collect_search_items", boom)

    merged, keyword_ids, stats = collect_news.collect_all_sources()

    assert len(merged) == 1
    assert keyword_ids == []  # 마킹 스킵 → 다음 실행에서 재검색
    assert stats["failed_sources"] == ["search"]


def test_collect_all_sources_raises_when_both_fail(monkeypatch):
    def boom():
        raise RuntimeError("down")

    monkeypatch.setattr(collect_news, "collect_headline_items", boom)
    monkeypatch.setattr(collect_news, "collect_search_items", boom)

    with pytest.raises(RuntimeError, match="모두 실패"):
        collect_news.collect_all_sources()
