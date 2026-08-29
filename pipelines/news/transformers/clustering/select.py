"""클러스터링 결과에서 저장 대상 기사를 선별하는 상위 헬퍼.

코어(cluster.py)는 인덱스만 다루고, 이 모듈이 기사 dict 목록과 코어를 잇는다.
"""

from __future__ import annotations

from typing import Any

from pipelines.news.transformers.clustering.cluster import build_clusters, select_top_members
from pipelines.news.transformers.clustering.preprocess import document_terms
from pipelines.news.transformers.clustering.vectorize import build_tfidf


def select_articles_by_cluster(
    items: list[dict[str, Any]],
    threshold: float,
    description_weight: float,
    max_per_cluster: int,
) -> tuple[list[list[dict[str, Any]]], dict[str, int]]:
    """제목+description 기반으로 군집을 만들고 군집당 최대 N개만 남긴다.

    반환되는 각 그룹은 대표(메도이드)가 첫 번째인 기사 목록이다. 단독 기사는
    크기 1 그룹이 된다. 그룹 순서는 저장 후 cluster_rep_news_id 기록에 쓰인다.
    """

    if not items:
        return [], {"collected": 0, "cluster_count": 0, "selected": 0, "dropped_by_cluster_cap": 0}

    documents = [
        document_terms(
            item.get("title", ""),
            item.get("description", ""),
            description_weight,
        )
        for item in items
    ]
    similarity = build_tfidf(documents).cosine_similarity()
    clusters = build_clusters(similarity, threshold)

    # select_top_members는 대표를 첫 번째로 돌려주므로 그룹 내 순서가 곧 대표 우선순위다
    groups = [
        [items[i] for i in select_top_members(cluster, similarity, cap=max_per_cluster)]
        for cluster in clusters
    ]

    selected_count = sum(len(group) for group in groups)

    stats = {
        "collected": len(items),
        "cluster_count": len(clusters),
        "selected": selected_count,
        "dropped_by_cluster_cap": len(items) - selected_count,
    }

    return groups, stats
