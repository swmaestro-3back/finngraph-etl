"""기사 → 서브벡터별 TF 토큰 가중치 (논문의 문서 표현). 순수 함수만.

저장되는 서브벡터는 title·body·entities 세 개다. all(제목+본문)은 TF 가 선형이라 title+body 합으로
그때 만든다 — 문서와 클러스터 centroid 둘 다 with_all 로 네 필드를 만든다. entities 는 외부 NER 대신
news_companies 의 company_id 다. jsonb 키는 문자열이라 company_id 도 문자열로 둔다.
"""

from __future__ import annotations

from collections.abc import Iterable

from pipelines.news.transformers.clustering.preprocess import tokenize

FIELDS = ("title", "body", "all", "entities")
STORED_FIELDS = ("title", "body", "entities")

# 필드 → (토큰 → TF 가중치). news.term_weights·news_clusters.centroids 의 파이썬 형태다.
TermWeights = dict[str, dict[str, float]]


def sum_terms(*term_lists: Iterable[tuple[str, float]]) -> dict[str, float]:
    """(토큰, 가중치) 목록들을 토큰별 합으로 접는다."""
    summed: dict[str, float] = {}
    for terms in term_lists:
        for token, weight in terms:
            summed[token] = summed.get(token, 0.0) + weight
    return summed


def build_term_weights(title: str, body: str, company_ids: Iterable[int]) -> TermWeights:
    """기사 한 건의 저장용 서브벡터. 본문은 전체를 쓴다."""
    return {
        "title": sum_terms(tokenize(title)),
        "body": sum_terms(tokenize(body)),
        "entities": {str(int(company_id)): 1.0 for company_id in company_ids},
    }


def with_all(weights: TermWeights) -> TermWeights:
    """저장 필드 세 개에 all(title+body 합)을 붙인 사본. 빠진 필드는 빈 dict 다."""
    full: TermWeights = {field: dict(weights.get(field) or {}) for field in STORED_FIELDS}
    full["all"] = sum_terms(full["title"].items(), full["body"].items())
    return full
