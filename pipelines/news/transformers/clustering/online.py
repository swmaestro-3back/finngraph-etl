"""
온라인 클러스터 판정.

저장됐지만 아직 판정되지 않은 기사를 발행 시각순으로 하나씩 본다. 시간 창 안에 있는 클러스터의
프로필과 코사인 유사도를 구해, 가장 높은 값이 threshold 이상이면 그 클러스터에 편입하고 아니면
새 클러스터를 만든다. DB 를 모르는 순수 함수만 둔다 — 시드와 IDF 표를 인자로 받는다.

프로필(term_weights)은 후보 기사들의 토큰 가중치 원시 합이다. 벡터로 만들 때는 member_count 로
나눠 "평균 기사 한 건" 스케일로 되돌린다 — 합을 그대로 log1p 에 넣으면 기사가 쌓일수록 기업명
토큰 하나가 벡터를 지배해, 같은 기업의 다른 사건까지 빨려 들어간다.

클러스터는 대표가 없고 후보가 promote_size 건 미만일 때만 후보를 받는다. 그 뒤에 붙는 기사는
count 만 올리고 프로필을 바꾸지 않는다. 한 런에 같은 사건 기사가 수십 건 들어와도 후보는
promote_size 건에서 멈춘다.

시간 창의 기준점은 클러스터를 만든 기사의 발행 시각이고 옮기지 않는다. 기사는 기준점 1일 전부터
7일 뒤 사이에 발행된 것만 붙는다(backward_days, window_days).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pipelines.news.transformers.clustering.vectorize import IdfTable, cosine, vectorize

# (토큰, 가중치) 목록. preprocess.document_terms 의 반환형이다.
Terms = list[tuple[str, float]]

SECONDS_PER_DAY = 86400.0


@dataclass(slots=True)
class ClusterSeed:
    """시간 창 안 기존 클러스터의 판정용 스냅샷 (news_clusters 한 행)."""

    cluster_id: int
    term_weights: dict[str, float]
    # 프로필에 합쳐진 후보 기사 수
    member_count: int
    # 대표 기사가 정해졌는가 (representative_news_id IS NOT NULL)
    promoted: bool
    # 클러스터를 만든 기사의 발행 시각. 시간 창의 기준점이다.
    first_published_at: datetime


@dataclass(slots=True)
class ClusterAssignment:
    """한 클러스터가 이번 판정에서 받은 기사. 인덱스는 assign_online 의 documents 기준이다."""

    seed: ClusterSeed | None  # None 이면 새 클러스터
    candidates: list[int]  # 후보로 편입돼 프로필에 합쳐진 기사 (발행 시각순)
    followers: list[int]  # count 만 올리는 기사 (발행 시각순)
    term_weights: dict[str, float]  # 편입 후 프로필 전체. 후보가 없으면 시드의 것 그대로다
    first_published_at: datetime  # 기준점. 시드면 시드의 값이다
    last_published_at: datetime  # 이번에 받은 기사 중 가장 늦은 발행 시각


@dataclass(slots=True)
class _Working:
    """판정 도중의 클러스터 상태. 시드와 이번 런에서 만든 클러스터를 같은 모양으로 다룬다."""

    seed: ClusterSeed | None
    term_weights: dict[str, float]
    member_count: int
    promoted: bool
    first_published_at: datetime
    first_epoch: float
    vector: dict[str, float]
    candidates: list[int]
    followers: list[int]
    last_published_at: datetime | None = None
    last_epoch: float | None = None


def sum_terms(*term_lists: Iterable[tuple[str, float]]) -> dict[str, float]:
    """(토큰, 가중치) 목록들을 토큰별 합으로 접는다."""
    summed: dict[str, float] = {}
    for terms in term_lists:
        for token, weight in terms:
            summed[token] = summed.get(token, 0.0) + weight
    return summed


def top_keywords(term_weights: dict[str, float], count: int) -> list[str]:
    """원시 가중치 상위 count개 토큰. 가중치가 같으면 토큰 사전순이다.

    IDF 를 섞지 않는다 — df 표가 바뀔 때마다 같은 클러스터의 키워드가 달라지기 때문이다.
    """
    ranked = sorted(term_weights.items(), key=lambda kv: (-kv[1], kv[0]))
    return [token for token, _ in ranked[:count]]


def profile_vector(
    term_weights: dict[str, float], member_count: int, idf: IdfTable
) -> dict[str, float]:
    """프로필을 평균 기사 한 건 스케일로 되돌려 벡터로 만든다. 빈 프로필은 빈 벡터다."""
    if not term_weights or member_count <= 0:
        return {}
    scaled_terms = ((token, weight / member_count) for token, weight in term_weights.items())
    return vectorize(scaled_terms, idf)


def assign_online(
    documents: list[Terms],
    published_ats: list[datetime],
    seeds: list[ClusterSeed],
    idf: IdfTable,
    *,
    threshold: float,
    promote_size: int,
    window_days: float,
    backward_days: float,
) -> list[ClusterAssignment]:
    """새 기사를 발행 시각순으로 기존 클러스터(seeds)에 편입시키거나 새 클러스터로 만든다.

    기사를 하나도 받지 않은 시드는 결과에 없다. 결과 순서는 시드(입력 순서) 다음에 새 클러스터
    (만들어진 순서)다. 유사도가 같으면 먼저 있던 클러스터가 이긴다.
    """
    if len(documents) != len(published_ats):
        raise ValueError("documents 와 published_ats 길이가 다릅니다.")
    if not documents:
        return []

    forward = window_days * SECONDS_PER_DAY
    backward = backward_days * SECONDS_PER_DAY

    clusters = [
        _Working(
            seed=seed,
            term_weights=dict(seed.term_weights),
            member_count=seed.member_count,
            promoted=seed.promoted,
            first_published_at=seed.first_published_at,
            first_epoch=seed.first_published_at.timestamp(),
            vector=profile_vector(seed.term_weights, seed.member_count, idf),
            candidates=[],
            followers=[],
        )
        for seed in seeds
    ]
    # 창이 아직 닫히지 않은 클러스터. 기사를 발행 시각순으로 보므로 한 번 닫힌 창은 다시 열리지
    # 않는다 — 닫힌 클러스터를 여기서 빼 두면 기사마다 전체 클러스터를 훑지 않는다.
    active = list(clusters)

    epochs = [moment.timestamp() for moment in published_ats]
    for index in sorted(range(len(documents)), key=lambda i: (epochs[i], i)):
        epoch = epochs[index]
        active = [cluster for cluster in active if cluster.first_epoch + forward >= epoch]
        vector = vectorize(documents[index], idf)

        best: _Working | None = None
        best_similarity = 0.0
        for cluster in active:
            if epoch < cluster.first_epoch - backward:
                continue
            similarity = cosine(vector, cluster.vector)
            if similarity >= threshold and similarity > best_similarity:
                best, best_similarity = cluster, similarity

        if best is None:
            best = _Working(
                seed=None,
                term_weights={},
                member_count=0,
                promoted=False,
                first_published_at=published_ats[index],
                first_epoch=epoch,
                vector={},
                candidates=[],
                followers=[],
            )
            clusters.append(best)
            active.append(best)

        if not best.promoted and best.member_count < promote_size:
            best.term_weights = sum_terms(best.term_weights.items(), documents[index])
            best.member_count += 1
            best.vector = profile_vector(best.term_weights, best.member_count, idf)
            best.candidates.append(index)
        else:
            best.followers.append(index)

        if best.last_epoch is None or epoch > best.last_epoch:
            best.last_epoch = epoch
            best.last_published_at = published_ats[index]

    return [
        ClusterAssignment(
            seed=cluster.seed,
            candidates=cluster.candidates,
            followers=cluster.followers,
            term_weights=cluster.term_weights,
            first_published_at=cluster.first_published_at,
            last_published_at=cluster.last_published_at,
        )
        for cluster in clusters
        if cluster.candidates or cluster.followers
    ]
