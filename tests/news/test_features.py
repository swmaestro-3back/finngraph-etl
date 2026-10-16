"""유사도 피처(features.py) 단위 테스트 — 서브벡터 코사인 4개 + 가우시안 시간 피처 3개 + 가중합."""

from __future__ import annotations

import math

import pytest

from pipelines.news.transformers.clustering.features import (
    TEXT_FEATURES,
    TIME_FEATURES,
    score,
    similarity_features,
    time_similarity,
)


def test_feature_names_and_order():
    assert TEXT_FEATURES == ("title", "body", "all", "entities")
    assert TIME_FEATURES == ("last", "mean", "first")


def test_time_similarity_is_a_gaussian_in_hours():
    assert time_similarity(0.0, 72.0) == pytest.approx(1.0)
    assert time_similarity(72.0, 72.0) == pytest.approx(math.exp(-0.5))
    assert time_similarity(-72.0, 72.0) == time_similarity(72.0, 72.0)
    assert time_similarity(216.0, 72.0) < 0.02  # 3σ 에서는 사실상 0


def test_time_similarity_rejects_non_positive_sigma():
    with pytest.raises(ValueError):
        time_similarity(1.0, 0.0)


def test_similarity_features_has_seven_keys_and_zero_for_missing_vectors():
    doc = {"title": {"a": 1.0}, "body": {}, "all": {"a": 1.0}, "entities": {}}
    cluster = {"title": {"a": 1.0}, "all": {"a": 0.6, "b": 0.8}}

    features = similarity_features(
        doc, cluster, 10.0, {"last": 10.0, "mean": 9.0, "first": 8.0}, sigma_hours=72.0
    )

    assert set(features) == set(TEXT_FEATURES) | set(TIME_FEATURES)
    assert features["title"] == pytest.approx(1.0)
    assert features["all"] == pytest.approx(0.6)
    assert features["body"] == 0.0 and features["entities"] == 0.0  # 빈 벡터는 무엇과도 0
    assert features["last"] == pytest.approx(1.0)
    assert features["mean"] == pytest.approx(time_similarity(1.0, 72.0))
    assert features["first"] == pytest.approx(time_similarity(2.0, 72.0))


def test_score_is_the_weighted_sum_in_feature_order():
    features = {name: 1.0 for name in TEXT_FEATURES + TIME_FEATURES}
    assert score(features, (1.0, 1.0, 1.0, 1.0), (0.1, 0.1, 0.1)) == pytest.approx(4.3)

    features.update({"title": 0.5, "body": 0.0, "all": 0.25, "entities": 1.0})
    # 0.5×2 + 0 + 0.25×4 + 1×1 + 시간 0.3
    assert score(features, (2.0, 1.0, 4.0, 1.0), (0.1, 0.1, 0.1)) == pytest.approx(3.3)


def test_score_rejects_wrong_weight_lengths():
    features = {name: 1.0 for name in TEXT_FEATURES + TIME_FEATURES}
    with pytest.raises(ValueError):
        score(features, (1.0, 1.0, 1.0), (0.1, 0.1, 0.1))
    with pytest.raises(ValueError):
        score(features, (1.0, 1.0, 1.0, 1.0), (0.1, 0.1))
