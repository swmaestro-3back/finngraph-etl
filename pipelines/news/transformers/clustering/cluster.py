"""
코사인 유사도 기반 average-link 병합 클러스터링.

계층 병합을 threshold 에서 잘라내는 방식이라 클러스터 개수를 미리 정할 필요가
없다. 어떤 기사가 몇 개의 사건으로 묶일지 모르는 뉴스에 맞는 성질이다.

배치를 넘는 클러스터링(incremental.py)은 기존 클러스터의 프로필을 시드 행으로 앞에
심고 같은 병합을 돌린다. 시드끼리는 합치지 않으므로(seed_flags) 기존 클러스터는
새 기사를 받아들이기만 한다.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

DEFAULT_THRESHOLD = 0.35


@dataclass(slots=True)
class Cluster:
    members: list[int]
    representative: int
    cohesion: float


def agglomerative(
    similarity: np.ndarray,
    threshold: float = DEFAULT_THRESHOLD,
    initial_sizes: Sequence[float] | None = None,
    seed_flags: Sequence[bool] | None = None,
) -> list[list[int]]:
    """평균 연결 유사도가 threshold 이상인 두 군집을 반복해서 합친다.

    initial_sizes: 행별 초기 크기. 시드 행은 기사 여러 건을 대표하므로 병합 시 크기 가중
      평균에서 그만큼의 무게를 갖는다. None 이면 전부 1 이다.
    seed_flags: True 인 행은 기존 클러스터의 시드다. 시드끼리는 합치지 않으며, 시드를
      흡수한 군집도 계속 시드로 취급해 다른 시드와 합쳐지지 않는다.
    """
    n = similarity.shape[0]
    if n == 0:
        return []

    if initial_sizes is not None and len(initial_sizes) != n:
        raise ValueError("initial_sizes 길이가 similarity 크기와 다릅니다.")
    if seed_flags is not None and len(seed_flags) != n:
        raise ValueError("seed_flags 길이가 similarity 크기와 다릅니다.")

    groups: dict[int, list[int]] = {i: [i] for i in range(n)}
    sizes = (
        np.ones(n, dtype=np.float64)
        if initial_sizes is None
        else np.asarray(initial_sizes, dtype=np.float64)
    )
    is_seed = np.zeros(n, dtype=bool) if seed_flags is None else np.asarray(seed_flags, dtype=bool)
    # 군집 간 평균 유사도. 병합할 때마다 크기 가중 평균으로 갱신한다.
    linkage = similarity.astype(np.float64).copy()
    np.fill_diagonal(linkage, -1.0)
    active = np.ones(n, dtype=bool)

    while True:
        allowed = np.outer(active, active) & ~np.outer(is_seed, is_seed)
        masked = np.where(allowed, linkage, -1.0)
        best = int(np.argmax(masked))
        i, j = divmod(best, n)
        if masked[i, j] < threshold:
            break

        size_i, size_j = sizes[i], sizes[j]
        merged = (linkage[i] * size_i + linkage[j] * size_j) / (size_i + size_j)

        groups[i] = groups[i] + groups[j]
        del groups[j]
        active[j] = False
        sizes[i] = size_i + size_j
        is_seed[i] = is_seed[i] or is_seed[j]

        linkage[i, :] = merged
        linkage[:, i] = merged
        linkage[i, i] = -1.0

    return [sorted(members) for members in groups.values()]


def medoid_and_cohesion(similarity: np.ndarray, members: Sequence[int]) -> tuple[int, float]:
    """멤버 집합의 대표(메도이드)와 응집도를 계산한다.

    자기 자신과의 유사도(=1)를 뺀 평균으로 대표성을 재고, 그 평균들의 평균이 응집도다.
    멤버가 하나면 자신이 대표이고 응집도는 1.0 이다.
    """
    members = list(members)
    if len(members) == 1:
        return members[0], 1.0

    block = similarity[np.ix_(members, members)]
    avg = (block.sum(axis=1) - np.diag(block)) / (len(members) - 1)
    return members[int(np.argmax(avg))], float(avg.mean())


def build_clusters(
    similarity: np.ndarray,
    threshold: float = DEFAULT_THRESHOLD,
) -> list[Cluster]:
    """군집을 만들고, 각 군집의 대표 기사(메도이드)와 응집도를 계산한다."""
    clusters: list[Cluster] = []

    for members in agglomerative(similarity, threshold):
        representative, cohesion = medoid_and_cohesion(similarity, members)
        clusters.append(Cluster(members=members, representative=representative, cohesion=cohesion))

    # 큰 군집, 그 다음 응집도 높은 순.
    clusters.sort(key=lambda c: (len(c.members), c.cohesion), reverse=True)
    return clusters


def select_top_members(
    cluster: Cluster,
    similarity: np.ndarray,
    cap: int = 3,
) -> list[int]:
    """군집에서 저장할 기사를 최대 cap개 고른다.

    대표 기사(메도이드)를 항상 포함하고, 나머지는 대표와의 유사도가 높은 순으로
    채운다. 군집 크기가 cap 이하면 전부 돌려준다. 반환 목록은 항상 대표가
    첫 번째다.
    """

    if len(cluster.members) <= cap:
        representative = cluster.representative
        return [representative] + [m for m in cluster.members if m != representative]

    representative = cluster.representative
    others = [m for m in cluster.members if m != representative]
    others.sort(key=lambda m: float(similarity[representative, m]), reverse=True)

    return [representative] + others[: cap - 1]
