"""클러스터링 모듈 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

from pipelines.news.transformers.clustering.cluster import (
    Cluster,
    build_clusters,
    select_top_members,
)
from pipelines.news.transformers.clustering.vectorize import build_tfidf
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


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


def test_document_terms_drops_single_character_fragments():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("D램 가격 인상"))

    # "D램"이 쪼개진 "d" 조각은 버린다 — 복합명사 "d램"이 이미 그 정보를 담는다
    assert "d" not in terms
    assert terms.get("d램") == 3.0


def test_document_terms_drops_hanja():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("中 CXMT 반도체 증산"))

    # 한자는 매체마다 표기가 갈려 토큰으로 쓰지 않는다
    assert "中" not in terms
    assert set(terms) == {"cxmt", "반도체", "증산"}


def test_document_terms_does_not_build_compounds_across_hanja():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("철강株 수주"))

    # 한자가 명사 묶음을 끊어 "철강株" 복합명사가 만들어지지 않는다
    assert "철강株" not in terms
    assert "株" not in terms
    assert "철강" in terms


def test_document_terms_keeps_two_letter_abbreviations():
    from pipelines.news.transformers.clustering.preprocess import document_terms

    terms = dict(document_terms("SK하이닉스 실적 개선"))

    # 두 글자 영문 약어는 한 글자 규칙과 무관하다
    assert "sk" in terms
    assert terms.get("sk하이닉스") == 3.0


def test_agglomerative_defaults_match_size_one_and_no_seeds():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    similarity = np.array(
        [
            [1.0, 0.9, 0.1],
            [0.9, 1.0, 0.1],
            [0.1, 0.1, 1.0],
        ]
    )

    plain = agglomerative(similarity, threshold=0.35)
    explicit = agglomerative(
        similarity, threshold=0.35, initial_sizes=[1, 1, 1], seed_flags=[False, False, False]
    )

    assert plain == explicit == [[0, 1], [2]]


def test_agglomerative_never_merges_two_seeds():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    # 시드 0·1 은 서로 매우 비슷하지만 합쳐지면 안 된다. 새 문서 2 는 시드 0 에 붙는다.
    similarity = np.array(
        [
            [1.0, 0.95, 0.8],
            [0.95, 1.0, 0.3],
            [0.8, 0.3, 1.0],
        ]
    )

    groups = agglomerative(
        similarity, threshold=0.35, initial_sizes=[1, 1, 1], seed_flags=[True, True, False]
    )

    assert groups == [[0, 2], [1]]


def test_agglomerative_seed_absorbing_group_stays_blocked_from_other_seeds():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    # 새 문서 2 가 시드 0 에 붙은 뒤, 그 군집과 시드 1 의 평균 유사도가 threshold 를 넘어도
    # 시드를 흡수한 군집은 계속 시드로 취급돼 시드 1 과 합쳐지지 않는다.
    similarity = np.array(
        [
            [1.0, 0.5, 0.9],
            [0.5, 1.0, 0.9],
            [0.9, 0.9, 1.0],
        ]
    )

    groups = agglomerative(
        similarity, threshold=0.35, initial_sizes=[1, 1, 1], seed_flags=[True, True, False]
    )

    assert sorted(groups) == [[0, 2], [1]] or sorted(groups) == [[0], [1, 2]]
    assert not any(0 in group and 1 in group for group in groups)


def test_agglomerative_seed_size_weights_linkage_update():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    # 시드 0(크기 3)에 문서 2 가 붙은 뒤 문서 3 과의 연결 유사도는 크기 가중 평균이다:
    # (0.0 * 3 + 0.8 * 1) / 4 = 0.2 < threshold 이므로 문서 3 은 따로 남는다.
    # 시드 크기가 1 이었다면 (0.0 + 0.8) / 2 = 0.4 로 합쳐졌을 것이다.
    similarity = np.array(
        [
            [1.0, 0.0, 0.9, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.9, 0.0, 1.0, 0.8],
            [0.0, 0.0, 0.8, 1.0],
        ]
    )

    weighted = agglomerative(
        similarity,
        threshold=0.35,
        initial_sizes=[3, 1, 1, 1],
        seed_flags=[True, True, False, False],
    )
    unweighted = agglomerative(
        similarity,
        threshold=0.35,
        initial_sizes=[1, 1, 1, 1],
        seed_flags=[True, True, False, False],
    )

    assert weighted == [[0, 2], [1], [3]]
    assert unweighted == [[0, 2, 3], [1]]


def test_medoid_and_cohesion_matches_build_clusters():
    from pipelines.news.transformers.clustering.cluster import build_clusters, medoid_and_cohesion

    similarity = np.array(
        [
            [1.0, 0.9, 0.5, 0.0],
            [0.9, 1.0, 0.7, 0.0],
            [0.5, 0.7, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
        ]
    )

    [cluster, singleton] = build_clusters(similarity, threshold=0.35)
    representative, cohesion = medoid_and_cohesion(similarity, [0, 1, 2])

    assert (cluster.representative, cluster.cohesion) == (representative, cohesion)
    assert representative == 1  # 0·2 모두와 가장 가까운 문서
    assert singleton.cohesion == 1.0


# ── batch.py: 기사 → 문서 변환, 시드 창 ──────────────────────────


def test_batch_documents_parses_pub_date_and_falls_back():
    from pipelines.news.transformers.clustering import batch_documents

    fallback = datetime(2026, 9, 4, 10, tzinfo=SEOUL_TIMEZONE)
    items = [
        {
            "title": "삼성전자 유상증자 결정",
            "description": "삼성전자가 유상증자를 결정했다",
            "pubDate": "Tue, 02 Sep 2026 09:00:00 +0900",
        },
        {"title": "현대차 미국 리콜 확대", "description": "", "pubDate": "not-a-date"},
    ]

    documents, published_ats = batch_documents(
        items, description_weight=0.4, fallback_time=fallback
    )

    assert len(documents) == 2
    assert dict(documents[0])["삼성전자"] > dict(documents[1]).get("삼성전자", 0.0)
    assert published_ats[0] == datetime(2026, 9, 2, 9, tzinfo=SEOUL_TIMEZONE)
    assert published_ats[1] == fallback  # 파싱 실패는 실행 시각으로 본다


def test_seed_window_spans_batch_publish_range_minus_window():
    from pipelines.news.transformers.clustering import seed_window

    early = datetime(2026, 9, 1, 9, tzinfo=SEOUL_TIMEZONE)
    late = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)

    # 가장 이른 기사가 붙을 수 있는 가장 오래된 시드부터, 가장 늦은 기사 시각까지
    assert seed_window([late, early], 7) == (early - timedelta(days=7), late)


# ── 발행일 창 제약 ──────────────────────────────────────────────


def test_agglomerative_never_merges_pairs_wider_than_max_span():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    similarity = np.array([[1.0, 0.9], [0.9, 1.0]])

    within = agglomerative(similarity, 0.35, spans=[(0.0, 0.0), (5.0, 5.0)], max_span=7.0)
    beyond = agglomerative(similarity, 0.35, spans=[(0.0, 0.0), (8.0, 8.0)], max_span=7.0)

    assert within == [[0, 1]]
    assert beyond == [[0], [1]]


def test_agglomerative_span_is_checked_on_the_merged_group():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    # 0-1 (0일·5일) 은 합쳐지지만, 그 군집에 2 (10일) 를 더하면 0~10일이라 막힌다.
    # 1-2 만 보면 5일 차라 허용이지만 군집 전체 범위로 판단해야 한다.
    similarity = np.array(
        [
            [1.0, 0.9, 0.5],
            [0.9, 1.0, 0.9],
            [0.5, 0.9, 1.0],
        ]
    )

    groups = agglomerative(
        similarity, 0.35, spans=[(0.0, 0.0), (5.0, 5.0), (10.0, 10.0)], max_span=7.0
    )

    assert groups == [[0, 1], [2]]


def test_agglomerative_seed_anchor_rejects_articles_before_seed_start():
    from pipelines.news.transformers.clustering.cluster import agglomerative

    # 시드(0) 는 5일에 시작. 기사 1 은 3일(이전), 기사 2 는 9일(창 안).
    similarity = np.array(
        [
            [1.0, 0.9, 0.9],
            [0.9, 1.0, 0.1],
            [0.9, 0.1, 1.0],
        ]
    )

    groups = agglomerative(
        similarity,
        0.35,
        seed_flags=[True, False, False],
        spans=[(5.0, 5.0), (3.0, 3.0), (9.0, 9.0)],
        max_span=7.0,
    )

    assert groups == [[0, 2], [1]]
