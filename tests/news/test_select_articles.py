"""select_articles_by_cluster 순수 로직 단위 테스트 (API/DB 불필요)."""

from __future__ import annotations


def _item(title: str, description: str = "") -> dict:
    return {"title": title, "description": description, "link": f"https://x.com/{title}"}


def test_select_articles_by_cluster_caps_each_cluster():
    from pipelines.news.transformers.clustering import select_articles_by_cluster

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


def test_select_articles_by_cluster_first_is_medoid_when_under_cap():
    """cap 이하 군집(가장 흔한 경우)에서도 그룹 첫 번째는 대표(메도이드)여야 한다.

    대표(메도이드)가 입력 순서상 0번이 아닌 조합을 골라, select_top_members의
    early-return 분기(len(members) <= cap)가 인덱스 정렬 순서를 그대로 돌려주던
    회귀를 실제로 잡아낸다.
    """
    from pipelines.news.transformers.clustering import (
        build_clusters,
        build_tfidf,
        document_terms,
        select_articles_by_cluster,
    )

    # 이 3건은 threshold=0.35에서 한 군집으로 묶이고, 대표(메도이드)는 1번(0번이 아님)이다.
    items = [
        _item("삼성전자 대규모 유상증자 확정"),
        _item("삼성전자 유상증자 결정"),
        _item("삼성전자 유상증자 발표"),
    ]

    groups, _ = select_articles_by_cluster(
        items, threshold=0.35, description_weight=0.4, max_per_cluster=3
    )

    assert len(groups) == 1
    group = groups[0]
    assert len(group) == 3  # cap(3) 이하이므로 전부 유지 -> early-return 분기

    # select_articles_by_cluster와 동일한 절차로 대표(메도이드)를 독립적으로 재계산한다
    documents = [document_terms(item["title"], item["description"], 0.4) for item in items]
    similarity = build_tfidf(documents).cosine_similarity()
    [cluster] = build_clusters(similarity, threshold=0.35)
    assert cluster.representative != 0  # 대표가 입력상 첫 번째가 아님을 전제로 검증한다

    assert group[0]["title"] == items[cluster.representative]["title"]


def test_select_articles_by_cluster_handles_empty():
    from pipelines.news.transformers.clustering import select_articles_by_cluster

    groups, stats = select_articles_by_cluster(
        [], threshold=0.35, description_weight=0.4, max_per_cluster=3
    )

    assert groups == []
    assert stats["selected"] == 0
