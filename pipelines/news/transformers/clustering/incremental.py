"""
배치를 넘는 클러스터 판정.

이번 배치의 새 기사와 윈도우 안 기존 클러스터의 프로필을 한 코퍼스로 TF-IDF 를 만들고,
기존 클러스터를 시드로 심어 같은 average-link 병합을 돌린다. 결과는 새 기사 그룹마다
"기존 클러스터에 합류 / 새 클러스터" 와, 그 안에서 "저장 / cap 에 걸려 버림" 이다.
DB 를 모르는 순수 함수만 두어 로더 없이 테스트한다.

프로필(term_weights)은 판정된 모든 기사(저장 여부 무관)의 토큰 가중치 원시 합이다.
TF-IDF 에 넣을 때는 original_size 로 나눠 "평균 기사 한 건" 스케일로 되돌린다 — 합을
그대로 log1p 에 넣으면 기사가 쌓일수록 기업명 토큰 하나가 벡터를 지배해, 같은 기업의
다른 사건까지 빨려 들어간다.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime

from pipelines.news.transformers.clustering.cluster import (
    DEFAULT_THRESHOLD,
    Cluster,
    agglomerative,
    medoid_and_cohesion,
    select_top_members,
)
from pipelines.news.transformers.clustering.vectorize import build_tfidf
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

# (토큰, 가중치) 목록. preprocess.document_terms 의 반환형이다.
Terms = list[tuple[str, float]]


@dataclass(slots=True)
class ClusterSeed:
    """윈도우 안 기존 클러스터의 판정용 스냅샷 (news_clusters 한 행)."""

    cluster_id: int
    term_weights: dict[str, float]
    original_size: int
    member_count: int
    # 저장된 멤버 중 가장 늦은 보도일(KST). cap 의 "날짜가 바뀌면 1개 더" 판정에 쓴다.
    last_stored_date: date | None


@dataclass(slots=True)
class ClusterAssignment:
    """새 기사 그룹 하나의 판정 결과. 인덱스는 assign_batch 의 documents 기준이다."""

    seed: ClusterSeed | None  # None 이면 새 클러스터
    members: list[int]  # 이 클러스터로 판정된 새 기사 전체
    kept: list[int]  # 크롤링·저장 대상. 새 클러스터면 첫 번째가 메도이드
    dropped: list[int]  # cap 에 걸려 버리는 기사 (크롤링하지 않는다)
    term_weights: dict[str, float]  # members 전체의 토큰 합. 시드면 기존 프로필에 더할 증분
    first_published_at: datetime
    last_published_at: datetime
    cohesion: float | None  # 새 클러스터의 배치 내 응집도. 시드 합류는 저장 후 다시 계산


def sum_terms(*term_lists: Iterable[tuple[str, float]]) -> dict[str, float]:
    """(토큰, 가중치) 목록들을 토큰별 합으로 접는다."""
    summed: dict[str, float] = {}
    for terms in term_lists:
        for token, weight in terms:
            summed[token] = summed.get(token, 0.0) + weight
    return summed


def merge_term_weights(base: dict[str, float], extra: dict[str, float]) -> dict[str, float]:
    """두 프로필을 토큰별로 더한 새 dict 를 돌려준다. base 는 바꾸지 않는다."""
    return sum_terms(base.items(), extra.items())


def profile_terms(seed: ClusterSeed) -> Terms:
    """프로필을 평균 기사 스케일의 (토큰, 가중치) 목록으로 바꾼다. 빈 프로필은 빈 벡터다."""
    if not seed.term_weights or seed.original_size <= 0:
        return []
    return [(token, weight / seed.original_size) for token, weight in seed.term_weights.items()]


def top_keywords(term_weights: dict[str, float], count: int) -> list[str]:
    """원시 가중치 상위 count개 토큰.

    IDF 를 섞으면 배치 구성에 따라 값이 흔들려 같은 클러스터의 키워드가 런마다 달라지므로
    프로필 자체만 본다. 가중치가 같으면 토큰 사전순이다.
    """
    ranked = sorted(term_weights.items(), key=lambda kv: (-kv[1], kv[0]))
    return [token for token, _ in ranked[:count]]


def local_date(moment: datetime) -> date:
    """보도 시각을 KST 날짜로 바꾼다. 로더의 last_stored_date 계산과 같은 기준이다."""
    return moment.astimezone(SEOUL_TIMEZONE).date()


def select_within_cap(
    candidates: list[tuple[int, date, float]],
    member_count: int,
    last_stored_date: date | None,
    cap: int,
) -> tuple[list[int], list[int]]:
    """기존 클러스터에 합류하는 기사 중 저장할 것을 고른다.

    candidates 는 (문서 인덱스, 보도일, 시드와의 유사도). 누적 cap 까지 남은 자리는 보도일과
    무관하게 유사도 높은 순으로 채운다. 그 뒤에는 마지막 저장 기사보다 늦은 보도일마다
    유사도 최고 기사 1개만 더 받는다 — 같은 날 쏟아지는 동일 보도는 중복이지만, 다음 날의
    기사는 후속 전개일 가능성이 높다. (저장 목록, 버림 목록)을 돌려준다.
    """
    by_similarity = sorted(candidates, key=lambda c: -c[2])
    room = max(cap - member_count, 0)
    kept = by_similarity[:room]
    remaining = by_similarity[room:]

    stored_dates = [c[1] for c in kept] + ([last_stored_date] if last_stored_date else [])
    last_date = max(stored_dates) if stored_dates else None

    dropped: list[tuple[int, date, float]] = []
    for candidate in sorted(remaining, key=lambda c: (c[1], -c[2])):
        if last_date is None or candidate[1] > last_date:
            kept.append(candidate)
            last_date = candidate[1]
        else:
            dropped.append(candidate)

    return [c[0] for c in kept], [c[0] for c in dropped]


def pick_representative(member_terms: list[Terms]) -> tuple[int, float]:
    """저장된 멤버들만으로 TF-IDF 를 만들어 메도이드 인덱스와 응집도를 돌려준다.

    시드 클러스터에 멤버가 더해질 때 쓴다. 기존 멤버는 이번 배치의 유사도 행렬에 없으므로
    (프로필 행 하나로만 참여) 멤버 벡터를 다시 만들어 작은 코퍼스 안에서 비교한다.
    """
    if not member_terms:
        raise ValueError("멤버가 없습니다.")
    if len(member_terms) == 1:
        return 0, 1.0

    similarity = build_tfidf(member_terms).cosine_similarity()
    return medoid_and_cohesion(similarity, range(len(member_terms)))


def assign_batch(
    documents: list[Terms],
    published_ats: list[datetime],
    seeds: list[ClusterSeed],
    threshold: float = DEFAULT_THRESHOLD,
    cap: int = 3,
) -> list[ClusterAssignment]:
    """새 기사들을 기존 클러스터(seeds)에 합류시키거나 새 클러스터로 묶는다.

    시드 행을 코퍼스 앞에 두고 한 번의 병합을 돌리므로, 새 기사끼리의 묶음과 기존
    클러스터 합류가 같은 threshold 로 한 번에 결정된다. 시드의 병합 가중치는 저장된
    멤버 수다 — 판정 누적 수(original_size)는 상한이 없어 큰 클러스터가 새 기사 신호를
    완전히 눌러 버린다. 새 기사가 하나도 안 붙은 시드는 결과에 없다.
    """
    if len(documents) != len(published_ats):
        raise ValueError("documents 와 published_ats 길이가 다릅니다.")
    if not documents:
        return []

    n_seeds = len(seeds)
    corpus = [profile_terms(seed) for seed in seeds] + documents
    similarity = build_tfidf(corpus).cosine_similarity()
    groups = agglomerative(
        similarity,
        threshold,
        initial_sizes=[float(max(seed.member_count, 1)) for seed in seeds] + [1.0] * len(documents),
        seed_flags=[True] * n_seeds + [False] * len(documents),
    )

    assignments: list[ClusterAssignment] = []
    for group in groups:
        seed_rows = [row for row in group if row < n_seeds]
        doc_rows = [row for row in group if row >= n_seeds]
        if not doc_rows:
            continue

        indices = [row - n_seeds for row in doc_rows]
        cohesion: float | None = None

        if seed_rows:
            # 시드끼리는 합쳐지지 않으므로 시드 행은 정확히 하나다.
            [seed_row] = seed_rows
            seed = seeds[seed_row]
            candidates = [
                (
                    index,
                    local_date(published_ats[index]),
                    float(similarity[seed_row, index + n_seeds]),
                )
                for index in indices
            ]
            kept, dropped = select_within_cap(
                candidates, seed.member_count, seed.last_stored_date, cap
            )
        else:
            seed = None
            representative, cohesion = medoid_and_cohesion(similarity, doc_rows)
            cluster = Cluster(members=doc_rows, representative=representative, cohesion=cohesion)
            kept = [row - n_seeds for row in select_top_members(cluster, similarity, cap=cap)]
            kept_set = set(kept)
            dropped = [index for index in indices if index not in kept_set]

        published = [published_ats[index] for index in indices]
        assignments.append(
            ClusterAssignment(
                seed=seed,
                members=indices,
                kept=kept,
                dropped=dropped,
                term_weights=sum_terms(*(documents[index] for index in indices)),
                first_published_at=min(published),
                last_published_at=max(published),
                cohesion=cohesion,
            )
        )

    # 큰 그룹부터. 로그와 테스트에서 읽기 쉽다.
    assignments.sort(key=lambda assignment: len(assignment.members), reverse=True)
    return assignments
