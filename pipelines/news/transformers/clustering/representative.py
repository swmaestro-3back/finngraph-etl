"""
클러스터 대표 기사 선정.

후보 기사의 저장된 본문으로 고른다. 제목과 본문 토큰으로 후보끼리의 유사도를 구해, 다른
후보들과 가장 가까운 기사(메도이드)를 대표로 고른다 — 사건을 가장 전형적으로 다룬 기사다.
판정(online.py)은 제목과 본문 리드만 보지만, 대표는 본문 전체를 본다.
"""

from __future__ import annotations

from pipelines.news.transformers.clustering.preprocess import document_terms
from pipelines.news.transformers.clustering.vectorize import IdfTable, cosine, vectorize

# 본문 토큰을 제목과 같은 무게로 본다 — 대표는 본문이 충실한 기사여야 한다
BODY_WEIGHT = 1.0


def pick_representative(
    titles: list[str], bodies: list[str], idf: IdfTable, min_chars: int
) -> int | None:
    """대표 기사의 인덱스. 본문이 있는 후보가 하나도 없으면 None 이다.

    본문이 min_chars 이상인 후보만 본다. 그런 후보가 없으면 본문이 있는 후보 전부를 본다 —
    대표 없이 두는 것보다 짧은 본문이라도 고르는 편이 낫다. 유사도 합이 같으면 앞선 후보다.
    """
    if len(titles) != len(bodies):
        raise ValueError("titles 와 bodies 길이가 다릅니다.")

    with_body = [index for index, body in enumerate(bodies) if body and body.strip()]
    if not with_body:
        return None

    eligible = [index for index in with_body if len(bodies[index].strip()) >= min_chars]
    if not eligible:
        eligible = with_body
    if len(eligible) == 1:
        return eligible[0]

    vectors = {
        index: vectorize(document_terms(titles[index], bodies[index], BODY_WEIGHT), idf)
        for index in eligible
    }

    def centrality(index: int) -> float:
        return sum(cosine(vectors[index], vectors[other]) for other in eligible if other != index)

    return max(eligible, key=lambda index: (centrality(index), -index))
