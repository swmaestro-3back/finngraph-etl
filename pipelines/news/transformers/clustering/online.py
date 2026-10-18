"""온라인 클러스터 판정 (Miranda et al. 2018 의 단일 언어판).

저장됐지만 아직 판정되지 않은 기사를 발행 시각순으로 하나씩 본다. 후보 클러스터(가장 최신 멤버가
candidate_days 안)마다 서브벡터 4개(title·body·all·entities)의 코사인 φ 와 시간 피처 3개(최신·평균·
최고 멤버 시각과의 가우시안) s 를 구해 가중합 Γ 를 만들고, 가장 높은 Γ 가 threshold 를 넘으면 그
클러스터에 편입하고 아니면 새 클러스터를 만든다. DB 를 모르는 순수 함수만 둔다 — 시드와 IDF 표를
인자로 받는다.

centroids 는 멤버 기사 서브벡터 TF 가중치의 원시 합(title·body·entities)이다. 벡터로 만들 때
article_count 로 나눠 "평균 기사 한 건" 스케일로 되돌리고 all(=title+body) 을 붙여 IDF 를 곱한다 —
합을 그대로 log1p 에 넣으면 기사가 쌓일수록 기업명 토큰 하나가 벡터를 지배한다. cap 은 없다.
배정은 불변이다.

활성 집합은 last_epoch >= 기사 epoch − candidate_days 이고 first_epoch <= 기사 epoch +
candidate_days 이며 first_epoch >= 기사 epoch − max_span_days 다. 기사를 발행 시각순으로 보므로
한 번 빠진 클러스터는 돌아오지 않는다. 멤버가 계속 붙으면 last 가 밀려 살아 있지만 first 는 거의
움직이지 않으므로, 첫 기사로부터 max_span_days 가 지나면 끝난다 — 같은 기업 기사가 며칠에 한 번씩
나와 클러스터가 몇 달을 사는 체인을 여기서 끊는다. first 상한은 시드에만 작용한다 — 이번 런에서
만든 클러스터는 늘 기사보다 이르다.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

from pipelines.news.transformers.clustering.document import (
    STORED_FIELDS,
    TermWeights,
    sum_terms,
    with_all,
)
from pipelines.news.transformers.clustering.features import (
    SECONDS_PER_HOUR,
    score,
    similarity_features,
)
from pipelines.news.transformers.clustering.vectorize import IdfTables, vectorize_fields

__all__ = [
    "ClusterAssignment",
    "ClusterSeed",
    "add_document",
    "assign_online",
    "centroid_vectors",
    "empty_centroids",
    "sum_terms",
    "top_keywords",
]

SECONDS_PER_DAY = 86400.0

Vectors = dict[str, dict[str, float]]


@dataclass(slots=True)
class ClusterSeed:
    """후보 기존 클러스터의 판정용 스냅샷 (news_clusters 한 행)."""

    cluster_id: int
    centroids: TermWeights  # title·body·entities 합
    article_count: int
    first_published_at: datetime
    mean_published_at: datetime
    last_published_at: datetime


@dataclass(slots=True)
class ClusterAssignment:
    """한 클러스터가 이번 판정에서 받은 기사. 인덱스는 assign_online 의 documents 기준이다."""

    seed: ClusterSeed | None  # None 이면 새 클러스터
    members: list[int]  # 편입된 기사 (발행 시각순)
    scores: list[float | None]  # members 와 같은 순서의 Γ. 새 클러스터의 첫 기사는 None
    features: list[dict[str, float] | None]  # members 와 같은 순서의 피처 7개. 첫 기사는 None
    centroids: TermWeights  # 편입 후 합
    article_count: int
    first_published_at: datetime
    mean_published_at: datetime
    last_published_at: datetime


@dataclass(slots=True)
class _Working:
    seed: ClusterSeed | None
    centroids: TermWeights
    article_count: int
    first_epoch: float
    last_epoch: float
    epoch_sum: float
    vectors: Vectors
    members: list[int] = field(default_factory=list)
    scores: list[float | None] = field(default_factory=list)
    features: list[dict[str, float] | None] = field(default_factory=list)

    def hours(self) -> dict[str, float]:
        mean_epoch = self.epoch_sum / self.article_count if self.article_count else self.first_epoch
        return {
            "last": self.last_epoch / SECONDS_PER_HOUR,
            "mean": mean_epoch / SECONDS_PER_HOUR,
            "first": self.first_epoch / SECONDS_PER_HOUR,
        }


def empty_centroids() -> TermWeights:
    return {field_name: {} for field_name in STORED_FIELDS}


def add_document(centroids: TermWeights, document: TermWeights) -> TermWeights:
    """저장 필드마다 centroid 합에 문서 TF 를 더한 새 dict. 빠진 필드는 빈 dict 로 본다."""
    return {
        field_name: sum_terms(
            (centroids.get(field_name) or {}).items(), (document.get(field_name) or {}).items()
        )
        for field_name in STORED_FIELDS
    }


def top_keywords(terms: dict[str, float], count: int) -> list[str]:
    """원시 가중치 상위 count개 토큰. 가중치가 같으면 토큰 사전순이다."""
    ranked = sorted(terms.items(), key=lambda kv: (-kv[1], kv[0]))
    return [token for token, _ in ranked[:count]]


def centroid_vectors(centroids: TermWeights, article_count: int, idf_tables: IdfTables) -> Vectors:
    """합을 평균 기사 한 건 스케일로 되돌리고 all 을 붙여 네 필드 벡터로 만든다.

    article_count 가 0 이면 빈 dict 다.
    """
    if article_count <= 0:
        return {}
    scaled = {
        field_name: {token: weight / article_count for token, weight in terms.items()}
        for field_name, terms in with_all(centroids).items()
    }
    return vectorize_fields(scaled, idf_tables)


def _from_epoch(epoch: float, like: datetime) -> datetime:
    return datetime.fromtimestamp(epoch, tz=like.tzinfo)


def assign_online(
    documents: list[TermWeights],
    published_ats: list[datetime],
    seeds: list[ClusterSeed],
    idf_tables: IdfTables,
    *,
    text_weights: Sequence[float],
    time_weights: Sequence[float],
    sigma_hours: float,
    threshold: float,
    candidate_days: float,
    max_span_days: float,
) -> list[ClusterAssignment]:
    """새 기사를 발행 시각순으로 기존 클러스터(seeds)에 편입시키거나 새 클러스터로 만든다.

    기사를 하나도 받지 않은 시드는 결과에 없다. 결과 순서는 시드(입력 순서) 다음에 새 클러스터
    (만들어진 순서)다. Γ 가 같으면 먼저 있던 클러스터가 이긴다.
    """
    if len(documents) != len(published_ats):
        raise ValueError("documents 와 published_ats 길이가 다릅니다.")
    if not documents:
        return []

    cutoff = candidate_days * SECONDS_PER_DAY
    span_cutoff = max_span_days * SECONDS_PER_DAY
    clusters = [
        _Working(
            seed=seed,
            centroids=add_document(empty_centroids(), seed.centroids),
            article_count=seed.article_count,
            first_epoch=seed.first_published_at.timestamp(),
            last_epoch=seed.last_published_at.timestamp(),
            epoch_sum=seed.mean_published_at.timestamp() * seed.article_count,
            vectors=centroid_vectors(seed.centroids, seed.article_count, idf_tables),
        )
        for seed in seeds
    ]
    active = list(clusters)

    epochs = [moment.timestamp() for moment in published_ats]
    for index in sorted(range(len(documents)), key=lambda i: (epochs[i], i)):
        epoch = epochs[index]
        # 컷은 가우시안처럼 대칭이다: 끊긴 지 오래된 클러스터도, 기사보다 한참 뒤에 시작한 클러스터
        # (늦게 도착한 옛 기사의 경우)도 후보가 아니다 — 텍스트만으로 붙어 first 를 끌어내리지 않게.
        # 수명 컷(first >= 기사 − max_span_days)은 멤버가 계속 붙어 last 가 밀리는 체인을 끊는다
        active = [
            cluster
            for cluster in active
            if cluster.last_epoch >= epoch - cutoff
            and cluster.first_epoch <= epoch + cutoff
            and cluster.first_epoch >= epoch - span_cutoff
        ]
        doc_vectors = vectorize_fields(with_all(documents[index]), idf_tables)
        doc_hours = epoch / SECONDS_PER_HOUR

        best: _Working | None = None
        best_score: float | None = None
        best_features: dict[str, float] | None = None
        for cluster in active:
            features = similarity_features(
                doc_vectors, cluster.vectors, doc_hours, cluster.hours(), sigma_hours
            )
            gamma = score(features, text_weights, time_weights)
            if gamma > threshold and (best_score is None or gamma > best_score):
                best, best_score, best_features = cluster, gamma, features

        if best is None:
            best = _Working(
                seed=None,
                centroids=empty_centroids(),
                article_count=0,
                first_epoch=epoch,
                last_epoch=epoch,
                epoch_sum=0.0,
                vectors={},
            )
            clusters.append(best)
            active.append(best)

        best.centroids = add_document(best.centroids, documents[index])
        best.article_count += 1
        best.epoch_sum += epoch
        best.first_epoch = min(best.first_epoch, epoch)
        best.last_epoch = max(best.last_epoch, epoch)
        best.vectors = centroid_vectors(best.centroids, best.article_count, idf_tables)
        best.members.append(index)
        best.scores.append(best_score)
        best.features.append(best_features)

    return [
        ClusterAssignment(
            seed=cluster.seed,
            members=cluster.members,
            scores=cluster.scores,
            features=cluster.features,
            centroids=cluster.centroids,
            article_count=cluster.article_count,
            first_published_at=_from_epoch(cluster.first_epoch, published_ats[cluster.members[0]]),
            mean_published_at=_from_epoch(
                cluster.epoch_sum / cluster.article_count, published_ats[cluster.members[0]]
            ),
            last_published_at=_from_epoch(cluster.last_epoch, published_ats[cluster.members[0]]),
        )
        for cluster in clusters
        if cluster.members
    ]
