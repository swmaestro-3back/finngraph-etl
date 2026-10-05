"""
가중치가 붙은 토큰 목록을 전역 IDF 기반 TF-IDF 희소 벡터로 바꾼다.

문서 하나를 그때그때 벡터로 만들어 클러스터 프로필과 비교하므로 행렬을 만들지 않는다.
벡터는 토큰 → 값 dict 이고, 코사인 유사도는 겹치는 토큰의 곱을 더한 값이다.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class IdfTable:
    """토큰별 문서 수(df)와 전체 문서 수. 런 시작 때 DB 에서 한 번 읽어 런 내내 고정한다.

    배치 안에서 df 를 세면 한 사건이 배치를 채울 때 그 사건의 핵심 토큰이 가장 흔한 토큰이 돼
    같은 사건이 갈라진다. 누적 df 를 쓰면 문서 하나의 벡터가 배치 구성과 무관해진다.
    """

    document_frequency: dict[str, int] = field(default_factory=dict)
    document_count: int = 0

    def idf(self, token: str) -> float:
        """처음 보는 토큰은 df=0 으로 본다 — 가장 큰 값이다."""
        df = self.document_frequency.get(token, 0)
        return math.log((1.0 + self.document_count) / (1.0 + df)) + 1.0


def vectorize(terms: Iterable[tuple[str, float]], idf: IdfTable) -> dict[str, float]:
    """(토큰, 가중치) 목록을 L2 정규화된 희소 TF-IDF 벡터로 바꾼다. 토큰이 없으면 빈 dict 다.

    같은 토큰의 가중치를 더한 뒤 sublinear TF(log1p)를 걸고 IDF 를 곱한다.
    """
    summed: dict[str, float] = {}
    for token, weight in terms:
        summed[token] = summed.get(token, 0.0) + weight

    vector = {
        token: math.log1p(weight) * idf.idf(token) for token, weight in summed.items() if weight > 0
    }
    norm = math.sqrt(sum(value * value for value in vector.values()))
    if norm == 0:
        return {}
    return {token: value / norm for token, value in vector.items()}


def cosine(left: dict[str, float], right: dict[str, float]) -> float:
    """정규화된 두 희소 벡터의 코사인 유사도(내적). 빈 벡터는 무엇과도 0 이다."""
    if len(left) > len(right):
        left, right = right, left
    return sum(value * right.get(token, 0.0) for token, value in left.items())
