"""collect_news job 단위 테스트 (API/DB 불필요 — 전부 monkeypatch)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import pipelines.news.jobs.collect_news as collect_news
from pipelines.news.models import NewsArticle


def _item(title: str, description: str = "", link: str | None = None) -> dict:
    return {
        "title": title,
        "description": description,
        "link": link or f"https://n.news.naver.com/{title}",
        "originallink": "",
        "pubDate": "",
    }


def _article(
    title: str, source_type: str = "headline", link: str | None = None, **kwargs
) -> NewsArticle:
    return NewsArticle(
        title=title,
        link=link or f"https://n.news.naver.com/{title}",
        source_type=source_type,
        **kwargs,
    )


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


def test_collect_headline_items_merges_category_articles(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_anchor_headline_settings", lambda: None)
    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)

    def fake_collect(category_id, target_count, max_more_calls):
        article = _article(
            f"카테고리{category_id} 기사",
            link=f"https://n/{category_id}",
            category_id=category_id,
            category_name=f"카테고리{category_id}",
        )
        return [article], {}

    monkeypatch.setattr(collect_news, "collect_category_new_headlines", fake_collect)

    articles = collect_news.collect_headline_items()

    assert len(articles) == 2  # 카테고리 2개 × 1건
    assert all(isinstance(article, NewsArticle) for article in articles)
    assert all(article.source_type == "headline" for article in articles)
    assert {article.category_id for article in articles} == {101, 103}


def test_collect_search_items_returns_articles_and_marks_keywords(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_search_settings", lambda: None)
    monkeypatch.setattr(
        collect_news,
        "fetch_search_keywords",
        lambda: [{"id": 7, "keyword": "유상증자"}, {"id": 9, "keyword": "리콜"}],
    )
    monkeypatch.setattr(
        collect_news,
        "collect_search_news",
        lambda queries: (
            [_article(f"{q} 기사", source_type="search", search_keyword=q) for q in queries],
            {"queries": 2, "raw": 2, "duplicate_removed": 0, "collected": 2},
        ),
    )
    marked_calls: list[list[int]] = []
    monkeypatch.setattr(
        collect_news,
        "mark_keywords_searched",
        lambda ids: marked_calls.append(ids) or len(ids),
    )

    articles = collect_news.collect_search_items()

    assert all(isinstance(article, NewsArticle) for article in articles)
    assert all(article.source_type == "search" for article in articles)
    assert {article.search_keyword for article in articles} == {"유상증자", "리콜"}
    assert marked_calls == [[7, 9]]  # 수집 성공 직후 last_searched_at 마킹


def test_collect_search_items_empty_keywords(monkeypatch):
    monkeypatch.setattr(collect_news, "validate_search_settings", lambda: None)
    monkeypatch.setattr(collect_news, "fetch_search_keywords", lambda: [])
    marked_calls: list[list[int]] = []
    monkeypatch.setattr(
        collect_news,
        "mark_keywords_searched",
        lambda ids: marked_calls.append(ids) or len(ids),
    )

    articles = collect_news.collect_search_items()

    assert articles == []
    assert marked_calls == []


def test_collect_all_sources_merges_headline_first(monkeypatch):
    headline = [_article("헤드라인 기사")]
    search = [_article("검색 기사", source_type="search")]
    monkeypatch.setattr(collect_news, "collect_headline_items", lambda: headline)
    monkeypatch.setattr(collect_news, "collect_search_items", lambda: search)

    merged = collect_news.collect_all_sources()

    assert merged == headline + search


def test_collect_all_sources_continues_when_headline_fails(monkeypatch):
    def boom():
        raise RuntimeError("crawl down")

    monkeypatch.setattr(collect_news, "collect_headline_items", boom)
    monkeypatch.setattr(
        collect_news,
        "collect_search_items",
        lambda: [_article("검색 기사", source_type="search")],
    )

    merged = collect_news.collect_all_sources()

    assert [article.title for article in merged] == ["검색 기사"]


def test_collect_all_sources_continues_when_search_fails(monkeypatch):
    def boom():
        raise RuntimeError("api down")

    monkeypatch.setattr(collect_news, "collect_headline_items", lambda: [_article("헤드라인 기사")])
    monkeypatch.setattr(collect_news, "collect_search_items", boom)

    merged = collect_news.collect_all_sources()

    assert [article.title for article in merged] == ["헤드라인 기사"]


def test_collect_all_sources_raises_when_both_fail(monkeypatch):
    def boom():
        raise RuntimeError("down")

    monkeypatch.setattr(collect_news, "collect_headline_items", boom)
    monkeypatch.setattr(collect_news, "collect_search_items", boom)

    with pytest.raises(RuntimeError, match="모두 실패"):
        collect_news.collect_all_sources()


def test_apply_batch_filters_order_and_stats(monkeypatch):
    # DB 필터만 흉내: 마지막 1건을 기존 기사로 돌려보낸다
    monkeypatch.setattr(
        collect_news, "filter_new_news_by_db", lambda items: (items[:-1], items[-1:])
    )

    items = [
        _item("삼성전자 유상증자 결정", link="https://n/1"),
        _item("삼성전자 유상증자 결정", link="https://n/1"),  # URL 중복
        _item("[포토] 삼성전자 주총 현장", link="https://n/2"),  # 유형 필터 탈락 (-5점)
        _item("현대차 리콜 확대", link="https://n/3"),
        _item("기아 신차 출시", link="https://n/4"),  # DB 기존 처리(stub이 마지막 1건 반환)
    ]

    new_items, stats = collect_news.apply_batch_filters(items, official_source_threshold=0)

    assert [item["title"] for item in new_items] == ["삼성전자 유상증자 결정", "현대차 리콜 확대"]
    assert stats == {
        "merged": 5,
        "duplicate_removed": 1,
        "type_removed": 1,
        "db_existing_removed": 1,
        "new": 2,
    }


def test_run_wires_full_pipeline(monkeypatch):
    """run()이 수집→필터→클러스터→본문→저장→대표기록→후처리를 올바로 잇는지 검증."""

    # conftest가 OFFICIAL_SOURCE_THRESHOLD=1을 주입하므로 stub(threshold=0)으로 대체해야
    # 실제 기사유형 필터를 통과한다
    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)

    articles = [
        _article("삼성전자 유상증자 결정", link="https://n/1"),
        _article("현대차 리콜 확대", source_type="search", link="https://n/2"),
    ]
    monkeypatch.setattr(collect_news, "collect_all_sources", lambda: articles)
    monkeypatch.setattr(collect_news, "filter_new_news_by_db", lambda batch: (batch, []))

    def fake_enrich(selected):
        for item in selected:
            item["_article_body"] = "본문"
        return selected

    monkeypatch.setattr(collect_news, "enrich_items_with_article_body", fake_enrich)
    monkeypatch.setattr(collect_news, "has_article_body", lambda item: True)

    next_id = iter(range(101, 200))

    def fake_save(items, save_summary, skip_existing):
        assert save_summary is False and skip_existing is True
        for item in items:
            item["_news_id"] = next(next_id)
        return {"inserted_count": len(items), "updated_count": 0}

    monkeypatch.setattr(collect_news, "save_news_items", fake_save)

    rep_calls: list[list[list[int]]] = []
    monkeypatch.setattr(
        collect_news,
        "assign_cluster_representatives",
        lambda groups: rep_calls.append(groups) or sum(len(g) for g in groups),
    )
    source_calls: list[tuple[str, list[int]]] = []
    monkeypatch.setattr(
        collect_news,
        "mark_news_source_type",
        lambda ids, source_type: source_calls.append((source_type, ids)) or len(ids),
    )

    result = collect_news.run()

    # 서로 다른 사건 2건 → 그룹 2개, 전부 저장
    assert result["cluster"]["cluster_count"] == 2
    assert result["saved"]["inserted_count"] == 2
    assert result["clustered"] == 2
    assert rep_calls and len(rep_calls[0]) == 2

    by_source = dict(source_calls)
    assert set(by_source) == {"headline", "search"}
    assert len(by_source["headline"]) == 1 and len(by_source["search"]) == 1


def test_run_handles_empty_collection(monkeypatch):
    """양쪽 다 0건이어도(실패 아님) 예외 없이 0 카운트로 끝난다."""

    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)
    monkeypatch.setattr(collect_news, "collect_all_sources", lambda: [])
    monkeypatch.setattr(collect_news, "filter_new_news_by_db", lambda batch: (batch, []))
    monkeypatch.setattr(collect_news, "save_news_items", lambda **kwargs: {"inserted_count": 0})
    monkeypatch.setattr(collect_news, "assign_cluster_representatives", lambda groups: 0)
    monkeypatch.setattr(collect_news, "mark_news_source_type", lambda ids, source_type: 0)

    result = collect_news.run()

    assert result["cluster"]["selected"] == 0
    assert result["saved"]["inserted_count"] == 0


def test_run_cluster_representative_falls_back_when_medoid_body_fails(monkeypatch):
    """대표(메도이드) 본문 크롤링 실패 시, 다음으로 저장에 성공한 기사가 대표가 된다."""

    from pipelines.news.transformers.clustering import select_articles_by_cluster

    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)

    articles = [
        _article("삼성전자 대규모 유상증자 확정", link="https://n/1"),
        _article("삼성전자 유상증자 결정", link="https://n/2"),
        _article("삼성전자 유상증자 발표", link="https://n/3"),
    ]

    # test_select_articles.py와 동일한 절차로 대표(메도이드)를 독립적으로 파악한다:
    # 이 3건은 threshold=0.35에서 한 군집으로 묶이고 group[0]이 대표가 된다.
    settings = _stub_settings()
    groups, _ = select_articles_by_cluster(
        [article.to_pipeline_item() for article in articles],
        threshold=settings.cluster_threshold,
        description_weight=settings.cluster_description_weight,
        max_per_cluster=settings.cluster_max_articles,
    )
    assert len(groups) == 1
    [group] = groups
    assert len(group) == 3
    medoid_title, fallback_title = group[0]["title"], group[1]["title"]

    monkeypatch.setattr(collect_news, "collect_all_sources", lambda: articles)
    monkeypatch.setattr(collect_news, "filter_new_news_by_db", lambda batch: (batch, []))
    monkeypatch.setattr(collect_news, "enrich_items_with_article_body", lambda selected: selected)
    # 대표(메도이드)만 본문 크롤링 실패로 취급
    monkeypatch.setattr(
        collect_news, "has_article_body", lambda item: item["title"] != medoid_title
    )

    next_id = iter(range(201, 300))
    saved_by_title: dict[str, int] = {}

    def fake_save(items, save_summary, skip_existing):
        for item in items:
            item["_news_id"] = next(next_id)
            saved_by_title[item["title"]] = item["_news_id"]
        return {"inserted_count": len(items), "updated_count": 0}

    monkeypatch.setattr(collect_news, "save_news_items", fake_save)

    rep_calls: list[list[list[int]]] = []
    monkeypatch.setattr(
        collect_news,
        "assign_cluster_representatives",
        lambda groups: rep_calls.append(groups) or sum(len(g) for g in groups),
    )
    monkeypatch.setattr(collect_news, "mark_news_source_type", lambda ids, source_type: len(ids))

    collect_news.run()

    assert len(rep_calls) == 1
    [saved_groups] = rep_calls
    assert len(saved_groups) == 1  # 그룹 1개만 대표 기록 대상
    [saved_group] = saved_groups

    assert medoid_title not in saved_by_title  # 본문 실패로 저장되지 않음
    assert len(saved_group) == 2  # 실패 기사의 id는 포함되지 않음
    assert saved_group[0] == saved_by_title[fallback_title]  # 두 번째 멤버가 대체 대표가 됨
