"""
코사인 유사도 기반 average-link 병합 클러스터링.

계층 병합을 threshold 에서 잘라내는 방식이라 클러스터 개수를 미리 정할 필요가
없다. 어떤 기사가 몇 개의 사건으로 묶일지 모르는 뉴스에 맞는 성질이다.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

DEFAULT_THRESHOLD = 0.35


@dataclass(slots=True)
class Cluster:
    members: list[int]
    representative: int
    cohesion: float


def agglomerative(similarity: np.ndarray, threshold: float = DEFAULT_THRESHOLD) -> list[list[int]]:
    """평균 연결 유사도가 threshold 이상인 두 군집을 반복해서 합친다."""
    n = similarity.shape[0]
    if n == 0:
        return []

    groups: dict[int, list[int]] = {i: [i] for i in range(n)}
    # 군집 간 평균 유사도. 병합할 때마다 크기 가중 평균으로 갱신한다.
    linkage = similarity.astype(np.float64).copy()
    np.fill_diagonal(linkage, -1.0)
    active = np.ones(n, dtype=bool)

    while True:
        masked = np.where(np.outer(active, active), linkage, -1.0)
        best = int(np.argmax(masked))
        i, j = divmod(best, n)
        if masked[i, j] < threshold:
            break

        size_i, size_j = len(groups[i]), len(groups[j])
        merged = (linkage[i] * size_i + linkage[j] * size_j) / (size_i + size_j)

        groups[i] = groups[i] + groups[j]
        del groups[j]
        active[j] = False

        linkage[i, :] = merged
        linkage[:, i] = merged
        linkage[i, i] = -1.0

    return [sorted(members) for members in groups.values()]


def build_clusters(
    similarity: np.ndarray,
    threshold: float = DEFAULT_THRESHOLD,
) -> list[Cluster]:
    """군집을 만들고, 각 군집의 대표 기사(메도이드)와 응집도를 계산한다."""
    clusters: list[Cluster] = []

    for members in agglomerative(similarity, threshold):
        if len(members) == 1:
            clusters.append(Cluster(members=members, representative=members[0], cohesion=1.0))
            continue

        block = similarity[np.ix_(members, members)]
        # 자기 자신과의 유사도(=1)를 뺀 평균으로 대표성과 응집도를 잰다.
        avg = (block.sum(axis=1) - np.diag(block)) / (len(members) - 1)
        representative = members[int(np.argmax(avg))]
        cohesion = float(avg.mean())
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
    채운다. 군집 크기가 cap 이하면 전부 돌려준다.
    """

    if len(cluster.members) <= cap:
        return list(cluster.members)

    representative = cluster.representative
    others = [m for m in cluster.members if m != representative]
    others.sort(key=lambda m: float(similarity[representative, m]), reverse=True)

    return [representative] + others[: cap - 1]
