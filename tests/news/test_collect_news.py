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

    items = [
        _item("삼성전자 유상증자 결정", link="https://n/1") | {"_source_type": "headline"},
        _item("현대차 리콜 확대", link="https://n/2") | {"_source_type": "search"},
    ]
    monkeypatch.setattr(
        collect_news,
        "collect_all_sources",
        lambda: (
            items,
            [7],
            {"headline_collected": 1, "search_collected": 1, "failed_sources": []},
        ),
    )
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
    keyword_calls: list[list[int]] = []
    monkeypatch.setattr(
        collect_news,
        "mark_keywords_searched",
        lambda ids: keyword_calls.append(ids) or len(ids),
    )

    result = collect_news.run()

    # 서로 다른 사건 2건 → 그룹 2개, 전부 저장
    assert result["cluster"]["cluster_count"] == 2
    assert result["saved"]["inserted_count"] == 2
    assert result["clustered"] == 2
    assert result["keywords_marked"] == 1
    assert rep_calls and len(rep_calls[0]) == 2

    by_source = dict(source_calls)
    assert set(by_source) == {"headline", "search"}
    assert len(by_source["headline"]) == 1 and len(by_source["search"]) == 1
    assert keyword_calls == [[7]]


def test_run_handles_empty_collection(monkeypatch):
    """양쪽 다 0건이어도(실패 아님) 예외 없이 0 카운트로 끝난다."""

    monkeypatch.setattr(collect_news, "get_news_settings", _stub_settings)
    monkeypatch.setattr(
        collect_news,
        "collect_all_sources",
        lambda: ([], [], {"headline_collected": 0, "search_collected": 0, "failed_sources": []}),
    )
    monkeypatch.setattr(collect_news, "filter_new_news_by_db", lambda batch: (batch, []))
    monkeypatch.setattr(collect_news, "save_news_items", lambda **kwargs: {"inserted_count": 0})
    monkeypatch.setattr(collect_news, "assign_cluster_representatives", lambda groups: 0)
    monkeypatch.setattr(collect_news, "mark_news_source_type", lambda ids, source_type: 0)
    monkeypatch.setattr(collect_news, "mark_keywords_searched", lambda ids: 0)

    result = collect_news.run()

    assert result["cluster"]["selected"] == 0
    assert result["saved"]["inserted_count"] == 0
