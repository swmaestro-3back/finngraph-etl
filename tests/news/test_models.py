"""NewsArticle 경계 모델 단위 테스트."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipelines.news.models import NewsArticle, to_articles


def test_from_collected_item_maps_search_metadata():
    raw = {
        "title": "삼성전자 유상증자 결정",
        "description": "요약",
        "link": "https://n.news.naver.com/article/1",
        "originallink": "https://press.example.com/1",
        "pubDate": "Fri, 29 Aug 2026 09:00:00 +0900",
        "_search_keyword": "유상증자",
    }

    article = NewsArticle.from_collected_item(raw, source_type="search")

    assert article.title == "삼성전자 유상증자 결정"
    assert article.description == "요약"
    assert article.link == "https://n.news.naver.com/article/1"
    assert article.originallink == "https://press.example.com/1"
    assert article.pub_date == "Fri, 29 Aug 2026 09:00:00 +0900"
    assert article.source_type == "search"
    assert article.search_keyword == "유상증자"
    assert article.category_id is None
    assert article.category_name is None


def test_from_collected_item_maps_headline_metadata():
    raw = {
        "title": "코스피 사상 최고치",
        "description": "",
        "link": "https://n.news.naver.com/article/2",
        "originallink": "https://n.news.naver.com/article/2",
        "pubDate": "",
        "_category_id": 101,
        "_category_name": "경제",
    }

    article = NewsArticle.from_collected_item(raw, source_type="headline")

    assert article.source_type == "headline"
    assert article.category_id == 101
    assert article.category_name == "경제"
    assert article.search_keyword is None


def test_to_pipeline_item_produces_downstream_contract():
    article = NewsArticle(
        title="현대차 리콜 확대",
        link="https://n.news.naver.com/article/3",
        source_type="headline",
        category_id=103,
        category_name="증권",
    )

    item = article.to_pipeline_item()

    assert item == {
        "title": "현대차 리콜 확대",
        "description": "",
        "link": "https://n.news.naver.com/article/3",
        "originallink": "",
        "pubDate": "",
        "_source_type": "headline",
    }


def test_rejects_blank_title_and_link():
    with pytest.raises(ValidationError):
        NewsArticle(title="", link="https://n/1", source_type="search")

    with pytest.raises(ValidationError):
        NewsArticle(title="제목", link="", source_type="search")


def test_rejects_unknown_source_type():
    with pytest.raises(ValidationError):
        NewsArticle(title="제목", link="https://n/1", source_type="keyword")


def test_to_articles_converts_and_tags_source_type():
    raw_items = [
        {"title": "기사1", "link": "https://n/1"},
        {"title": "기사2", "link": "https://n/2", "_search_keyword": "리콜"},
    ]

    articles = to_articles(raw_items, "search")

    assert [article.title for article in articles] == ["기사1", "기사2"]
    assert all(article.source_type == "search" for article in articles)
    assert articles[1].search_keyword == "리콜"


def test_to_articles_skips_contract_violations():
    raw_items = [
        {"title": "정상 기사", "link": "https://n/1"},
        {"title": "", "link": "https://n/2"},  # 빈 제목 → 계약 위반
    ]

    articles = to_articles(raw_items, "headline")

    assert [article.title for article in articles] == ["정상 기사"]
