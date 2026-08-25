"""collect_and_cluster job의 순수 로직 단위 테스트 (API/DB 불필요)."""

from __future__ import annotations


def _item(title: str, description: str = "") -> dict:
    return {"title": title, "description": description, "link": f"https://x.com/{title}"}


def test_select_articles_by_cluster_caps_each_cluster():
    from pipelines.news.jobs.collect_and_cluster import select_articles_by_cluster

    # 같은 사건 4건 + 다른 사건 1건
    items = [
        _item("삼성전자 유상증자 결정"),
        _item("삼성전자 유상증자 발표"),
        _item("삼성전자 유상증자 공시"),
        _item("삼성전자 유상증자 단행"),
        _item("현대차 미국 리콜 확대"),
    ]

    groups, stats = select_articles_by_cluster(
        items, threshold=0.35, description_weight=0.4, max_per_cluster=3
    )

    flat_titles = [item["title"] for group in groups for item in group]
    assert "현대차 미국 리콜 확대" in flat_titles  # 단독 기사는 크기 1 그룹으로 유지

    samsung_group = next(g for g in groups if "삼성전자" in g[0]["title"])
    assert len(samsung_group) == 3  # 군집은 3개로 제한
    assert "삼성전자" in samsung_group[0]["title"]  # 첫 번째가 대표(메도이드)

    assert stats["collected"] == 5
    assert stats["selected"] == 4
    assert stats["dropped_by_cluster_cap"] == 1


def test_select_articles_by_cluster_handles_empty():
    from pipelines.news.jobs.collect_and_cluster import select_articles_by_cluster

    groups, stats = select_articles_by_cluster(
        [], threshold=0.35, description_weight=0.4, max_per_cluster=3
    )

    assert groups == []
    assert stats["selected"] == 0


def test_search_query_list_parsing(monkeypatch):
    monkeypatch.setenv("NEWS_SEARCH_QUERIES", "특징주, 공급계약 ,,")

    from pipelines.news.config import NewsSettings

    assert NewsSettings().search_query_list() == ["특징주", "공급계약"]
