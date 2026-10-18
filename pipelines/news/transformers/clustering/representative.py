"""
클러스터 대표 기사 선정.

후보 기사의 저장된 term_weights 로 고른다. 제목+본문(all) 벡터로 후보끼리의 유사도를 구해, 다른
후보들과 가장 가까운 기사(메도이드)를 대표로 고른다 — 사건을 가장 전형적으로 다룬 기사다. 본문을
다시 토큰화하지 않는다. 본문 길이는 저장된 length(body) 다.
"""

from __future__ import annotations

from pipelines.news.transformers.clustering.document import TermWeights, with_all
from pipelines.news.transformers.clustering.vectorize import IdfTable, cosine, vectorize


def pick_representative(
    term_weights_list: list[TermWeights],
    body_lengths: list[int],
    idf_all: IdfTable,
    min_chars: int,
) -> int | None:
    """대표 기사의 인덱스. 본문이 있는 후보가 하나도 없으면 None 이다.

    본문이 min_chars 이상인 후보만 본다. 그런 후보가 없으면 본문이 있는 후보 전부를 본다 —
    대표 없이 두는 것보다 짧은 본문이라도 고르는 편이 낫다. 유사도 합이 같으면 앞선 후보다.
    """
    if len(term_weights_list) != len(body_lengths):
        raise ValueError("term_weights_list 와 body_lengths 길이가 다릅니다.")

    with_body = [index for index, length in enumerate(body_lengths) if length and length > 0]
    if not with_body:
        return None

    eligible = [index for index in with_body if body_lengths[index] >= min_chars]
    if not eligible:
        eligible = with_body
    if len(eligible) == 1:
        return eligible[0]

    vectors = {
        index: vectorize(with_all(term_weights_list[index])["all"].items(), idf_all)
        for index in eligible
    }

    def centrality(index: int) -> float:
        return sum(cosine(vectors[index], vectors[other]) for other in eligible if other != index)

    return max(eligible, key=lambda index: (centrality(index), -index))
