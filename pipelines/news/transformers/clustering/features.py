"""문서 ↔ 클러스터 유사도 피처 (논문 8단계). 순수 함수만.

텍스트 피처 φ 는 서브벡터(title·body·all·entities)마다 문서 벡터와 클러스터 centroid 벡터의
코사인이다. 시간 피처 s 는 문서 발행 시각과 클러스터의 최신(last)·평균(mean)·최고(first) 멤버
시각의 차이(시간)를 가우시안 exp(-Δ²/2σ²) 에 넣은 값이다 (μ=0). 점수 Γ 는 두 묶음의 가중합이고
정규화하지 않는다 — 외부에서 SVM-Rank 로 뽑은 가중치를 그대로 받기 위해서다. 외부 가중치 학습
레포는 이 모듈과 같은 정의로 피처를 만들어야 한다.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

from pipelines.news.transformers.clustering.document import FIELDS
from pipelines.news.transformers.clustering.vectorize import cosine

TEXT_FEATURES = FIELDS
TIME_FEATURES = ("last", "mean", "first")
SECONDS_PER_HOUR = 3600.0

Vectors = dict[str, dict[str, float]]


def time_similarity(delta_hours: float, sigma_hours: float) -> float:
    """시간 차이(시간 단위)의 가우시안 유사도. 차이 0 이면 1, σ 에서 exp(-1/2), 멀어지면 0."""
    if sigma_hours <= 0:
        raise ValueError("sigma_hours 는 0 보다 커야 합니다.")
    return math.exp(-(delta_hours * delta_hours) / (2.0 * sigma_hours * sigma_hours))


def similarity_features(
    doc_vectors: Vectors,
    cluster_vectors: Vectors,
    doc_hours: float,
    cluster_hours: dict[str, float],
    sigma_hours: float,
) -> dict[str, float]:
    """피처 7개. 없는 필드의 벡터는 빈 벡터(코사인 0)다. cluster_hours 키는 TIME_FEATURES."""
    features = {
        field: cosine(doc_vectors.get(field, {}), cluster_vectors.get(field, {}))
        for field in TEXT_FEATURES
    }
    for name in TIME_FEATURES:
        features[name] = time_similarity(doc_hours - cluster_hours[name], sigma_hours)
    return features


def score(
    features: dict[str, float], text_weights: Sequence[float], time_weights: Sequence[float]
) -> float:
    """Γ = Σ φᵢ·q⁰ᵢ + Σ sⱼ·q¹ⱼ. 가중치는 TEXT_FEATURES·TIME_FEATURES 순서다."""
    if len(text_weights) != len(TEXT_FEATURES):
        raise ValueError(f"text_weights 는 {len(TEXT_FEATURES)}개여야 합니다: {TEXT_FEATURES}")
    if len(time_weights) != len(TIME_FEATURES):
        raise ValueError(f"time_weights 는 {len(TIME_FEATURES)}개여야 합니다: {TIME_FEATURES}")
    text = sum(
        features[name] * weight for name, weight in zip(TEXT_FEATURES, text_weights, strict=True)
    )
    time = sum(
        features[name] * weight for name, weight in zip(TIME_FEATURES, time_weights, strict=True)
    )
    return text + time
