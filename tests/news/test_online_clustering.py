"""온라인 클러스터 판정(online.py) 단위 테스트. idf_tables={} 는 모든 토큰의 IDF 가 1 이다."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipelines.news.transformers.clustering.features import TEXT_FEATURES, TIME_FEATURES
from pipelines.news.transformers.clustering.online import (
    ClusterSeed,
    add_document,
    assign_online,
    centroid_vectors,
    empty_centroids,
    sum_terms,
    top_keywords,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

IDF: dict = {}
SUPPLY = {
    "title": {"엘앤에프": 3.0, "양극재": 1.0, "공급": 1.0},
    "body": {"양극재": 1.0, "계약": 1.0},
    "entities": {"1": 1.0},
}
EARNINGS = {"title": {"삼성전자": 3.0, "실적": 1.0}, "body": {"실적": 1.0}, "entities": {"2": 1.0}}
EMPTY = {"title": {}, "body": {}, "entities": {}}
# 기본 설정값과 같다: 텍스트 1×4, 시간 0.1×3, σ=72h, τ=1.0, 후보 10일, 수명 10일
OPTIONS = {
    "text_weights": (1.0, 1.0, 1.0, 1.0),
    "time_weights": (0.1, 0.1, 0.1),
    "sigma_hours": 72.0,
    "threshold": 1.0,
    "candidate_days": 10,
    "max_span_days": 10,
}


def _at(day: int, hour: int = 9) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=SEOUL_TIMEZONE)


def _seed(document=SUPPLY, **overrides) -> ClusterSeed:
    fields = {
        "cluster_id": 1,
        "centroids": add_document(empty_centroids(), document),
        "article_count": 1,
        "first_published_at": _at(1),
        "mean_published_at": _at(1),
        "last_published_at": _at(1),
    }
    fields.update(overrides)
    return ClusterSeed(**fields)


def _assign(documents, published_ats, seeds=(), **overrides):
    options = dict(OPTIONS)
    options.update(overrides)
    return assign_online(documents, published_ats, list(seeds), IDF, **options)


def test_sum_terms_and_top_keywords():
    assert sum_terms([("a", 1.0), ("b", 2.0)], [("a", 0.5)]) == {"a": 1.5, "b": 2.0}
    assert top_keywords({"a": 1.0, "b": 3.0, "c": 3.0}, 2) == ["b", "c"]


def test_add_document_sums_stored_fields_only():
    centroids = add_document(add_document(empty_centroids(), SUPPLY), SUPPLY)
    assert set(centroids) == {"title", "body", "entities"}
    assert centroids["title"]["엘앤에프"] == 6.0 and centroids["entities"] == {"1": 2.0}
    # 필드가 빠진 문서도 더할 수 있다
    assert add_document(empty_centroids(), {"title": {"a": 1.0}})["body"] == {}


def test_centroid_vectors_scale_to_average_article():
    single = add_document(empty_centroids(), SUPPLY)
    triple = add_document(add_document(single, SUPPLY), SUPPLY)
    one = centroid_vectors(single, 1, IDF)
    three = centroid_vectors(triple, 3, IDF)

    assert set(one) == set(TEXT_FEATURES)
    for field in TEXT_FEATURES:
        assert one[field] == pytest.approx(three[field])
    assert one["all"]["양극재"] > one["all"]["계약"]  # all = title+body 합
    # article_count 0 이나 빈 centroids 는 빈 벡터 — 0 으로 나누지 않는다
    assert centroid_vectors(empty_centroids(), 0, IDF) == {}
    assert centroid_vectors({}, 2, IDF) == {field: {} for field in TEXT_FEATURES}


def test_empty_input_returns_nothing():
    assert _assign([], []) == []


def test_length_mismatch_raises():
    with pytest.raises(ValueError):
        _assign([SUPPLY], [])


def test_similar_documents_form_one_cluster_and_others_get_their_own():
    result = _assign([SUPPLY, SUPPLY, EARNINGS], [_at(1), _at(1, 10), _at(1, 11)])

    assert [a.members for a in result] == [[0, 1], [2]]
    first, second = result[0].scores
    assert first is None  # 새 클러스터의 첫 기사
    assert 4.0 < second <= 4.3  # 텍스트 4개 모두 1 + 시간 피처 ≈ 0.3
    assert result[0].features[0] is None
    assert set(result[0].features[1]) == set(TEXT_FEATURES) | set(TIME_FEATURES)
    assert result[0].article_count == 2 and result[1].article_count == 1
    assert result[0].first_published_at == _at(1) and result[0].last_published_at == _at(1, 10)
    assert result[0].mean_published_at == _at(1) + timedelta(minutes=30)
    assert result[1].scores == [None]


def test_time_alone_does_not_join_unrelated_articles():
    # 같은 시각의 무관한 기사: φ 전부 0, 시간 피처 3×0.1 = 0.3 < τ
    result = _assign([SUPPLY, EARNINGS], [_at(1), _at(1)])
    assert [a.members for a in result] == [[0], [1]]


def test_document_joins_seed_and_updates_centroids_and_times():
    seed = _seed()
    [assignment] = _assign([SUPPLY], [_at(3)], [seed])

    assert assignment.seed is seed
    assert assignment.members == [0] and assignment.scores[0] > 4.0
    assert assignment.article_count == 2
    assert assignment.centroids == add_document(seed.centroids, SUPPLY)
    assert assignment.first_published_at == _at(1)
    assert assignment.mean_published_at == _at(2)  # (9/1 + 9/3) / 2
    assert assignment.last_published_at == _at(3)


def test_earlier_document_moves_first_published_at():
    seed = _seed(first_published_at=_at(3), mean_published_at=_at(3), last_published_at=_at(3))
    [assignment] = _assign([SUPPLY], [_at(2)], [seed])
    assert assignment.seed is seed
    assert assignment.first_published_at == _at(2)
    assert assignment.last_published_at == _at(3)


def test_far_in_time_scores_lower_than_near_but_identical_text_still_joins():
    seed = _seed()
    [near] = _assign([SUPPLY], [_at(1, 10)], [_seed()])
    [far] = _assign([SUPPLY], [_at(1) + timedelta(days=8)], [seed])
    assert near.scores[0] > far.scores[0]
    assert far.seed is seed  # 텍스트 4.0 만으로도 τ=1.0 을 넘는다
    assert far.features[0]["last"] < 0.05  # Δ=192h: exp(-192²/2·72²) ≈ 0.03


def test_cluster_outside_candidate_days_is_not_a_candidate():
    seed = _seed()
    result = _assign([SUPPLY], [_at(1) + timedelta(days=10, seconds=1)], [seed])
    assert result[0].seed is None


def test_cluster_starting_after_candidate_days_is_not_a_candidate():
    # 신규 종목의 180일 lookback 처럼 옛 기사가 늦게 도착한 경우: 기사보다 candidate_days 넘게 뒤에
    # 시작한 클러스터는 텍스트가 같아도 후보가 아니다 — 컷은 가우시안처럼 대칭이다
    seed = _seed(first_published_at=_at(20), mean_published_at=_at(20), last_published_at=_at(20))
    result = _assign([SUPPLY], [_at(1)], [seed])
    assert result[0].seed is None
    # 10일 안이면 후보다
    near = _seed(first_published_at=_at(10), mean_published_at=_at(10), last_published_at=_at(10))
    assert _assign([SUPPLY], [_at(1)], [near])[0].seed is near


def test_cluster_stays_alive_while_members_keep_arriving_within_max_span():
    # 6일 간격으로 세 기사. 셋째는 첫째보다 12일 뒤지만 둘째가 last 를 밀어 여전히 후보다 —
    # 수명(max_span_days) 안이라면
    result = _assign([SUPPLY] * 3, [_at(1), _at(7), _at(13)], max_span_days=20)
    assert [a.members for a in result] == [[0, 1, 2]]


def test_cluster_older_than_max_span_stops_taking_members():
    # 같은 세 기사라도 수명 10일이면 셋째(첫째보다 12일 뒤)는 last 가 6일 전이어도 새 클러스터다.
    # 같은 종목 기사가 며칠에 한 번씩 나오면 last 가 계속 밀려 클러스터가 몇 달을 사는 체인을 끊는다
    result = _assign([SUPPLY] * 3, [_at(1), _at(7), _at(13)])
    assert [a.members for a in result] == [[0, 1], [2]]
    assert result[1].seed is None and result[1].first_published_at == _at(13)


def test_seed_older_than_max_span_is_not_a_candidate():
    # 시드도 같은 규칙: first 가 기사보다 max_span_days 넘게 이르면 last 가 가까워도 후보가 아니다
    seed = _seed(first_published_at=_at(1), mean_published_at=_at(6), last_published_at=_at(12))
    assert _assign([SUPPLY], [_at(13)], [seed])[0].seed is None
    assert _assign([SUPPLY], [_at(13)], [seed], max_span_days=15)[0].seed is seed


def test_threshold_blocks_joining():
    result = _assign([SUPPLY, SUPPLY], [_at(1), _at(1, 10)], threshold=10.0)
    assert [a.members for a in result] == [[0], [1]]


def test_text_weights_change_the_score():
    [assignment] = _assign([SUPPLY], [_at(1, 10)], [_seed()], text_weights=(2.0, 0.0, 0.0, 0.0))
    assert assignment.scores[0] == pytest.approx(2.0 + 0.3, abs=0.01)


def test_seed_without_new_documents_is_omitted():
    assert _assign([EARNINGS], [_at(2)], [_seed()])[0].seed is None


def test_document_joins_the_highest_scoring_cluster():
    near = _seed(cluster_id=1, document=SUPPLY)
    far = _seed(
        cluster_id=2, document={"title": {"엘앤에프": 3.0, "실적": 1.0}, "body": {}, "entities": {}}
    )
    [assignment] = _assign([SUPPLY], [_at(2)], [far, near])
    assert assignment.seed.cluster_id == 1


def test_ties_go_to_the_earlier_cluster():
    [assignment] = _assign([SUPPLY], [_at(2)], [_seed(cluster_id=1), _seed(cluster_id=2)])
    assert assignment.seed.cluster_id == 1


def test_documents_are_processed_in_published_order():
    result = _assign([SUPPLY, SUPPLY], [_at(2), _at(1)])
    assert result[0].members == [1, 0]
    assert result[0].first_published_at == _at(1)


def test_documents_without_tokens_never_match():
    result = _assign([EMPTY, SUPPLY, EMPTY], [_at(1), _at(1, 10), _at(1, 11)])
    assert [a.members for a in result] == [[0], [1], [2]]
