"""클러스터링 모듈 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

import numpy as np

from pipelines.news.transformers.clustering.cluster import (
    Cluster,
    build_clusters,
    select_top_members,
)
from pipelines.news.transformers.clustering.vectorize import build_tfidf


def test_select_top_members_caps_and_prefers_similar_to_representative():
    # 대표(0번)와의 유사도: 1번=0.9, 2번=0.5, 3번=0.8, 4번=0.3
    similarity = np.array(
        [
            [1.0, 0.9, 0.5, 0.8, 0.3],
            [0.9, 1.0, 0.4, 0.7, 0.2],
            [0.5, 0.4, 1.0, 0.4, 0.2],
            [0.8, 0.7, 0.4, 1.0, 0.2],
            [0.3, 0.2, 0.2, 0.2, 1.0],
        ]
    )
    cluster = Cluster(members=[0, 1, 2, 3, 4], representative=0, cohesion=0.5)

    selected = select_top_members(cluster, similarity, cap=3)

    # 대표 우선 + 대표와 유사도 높은 순 2개
    assert selected == [0, 1, 3]


def test_select_top_members_returns_all_when_under_cap():
    similarity = np.identity(2)
    cluster = Cluster(members=[0, 1], representative=1, cohesion=0.4)

    # 군집이 cap 이하여도 반환 목록의 첫 번째는 항상 대표(메도이드)여야 한다
    assert select_top_members(cluster, similarity, cap=3) == [1, 0]


def test_build_clusters_groups_similar_documents():
    # 문서 0·1은 같은 토큰, 문서 2는 전혀 다른 토큰 → 2개 군집
    documents = [
        [("삼성전자", 3.0), ("유상증자", 1.0)],
        [("삼성전자", 3.0), ("유상증자", 1.0)],
        [("현대차", 3.0), ("리콜", 1.0)],
    ]
    tfidf = build_tfidf(documents)
    clusters = build_clusters(tfidf.cosine_similarity(), threshold=0.35)

    sizes = sorted(len(c.members) for c in clusters)
    assert sizes == [1, 2]


def test_document_terms_builds_compound_nouns():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("LG에너지솔루션 유상증자 결정"))

    # 복합명사 복원: 붙어 있던 명사 조각이 하나로 이어져 가중치 3.0
    assert terms.get("lg에너지솔루션") == 3.0
