"""
가중치가 붙은 토큰 목록을 TF-IDF 행렬로 바꾼다.

scikit-learn 의존성을 늘리지 않으려고 numpy 로 직접 계산한다. 문서 수가
수백~수천 건 규모라 밀집 행렬로 충분하다.
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np


class TfidfMatrix:
    """L2 정규화된 TF-IDF 행렬과 어휘 사전을 함께 들고 다닌다."""

    def __init__(self, matrix: np.ndarray, vocabulary: list[str]) -> None:
        self.matrix = matrix
        self.vocabulary = vocabulary

    def cosine_similarity(self) -> np.ndarray:
        """행 벡터가 L2 정규화돼 있으므로 내적이 곧 코사인 유사도다."""
        return np.clip(self.matrix @ self.matrix.T, 0.0, 1.0)


def build_tfidf(documents: list[list[tuple[str, float]]], min_df: int = 1) -> TfidfMatrix:
    """(토큰, 가중치) 목록들로부터 TF-IDF 행렬을 만든다."""
    document_frequency: dict[str, int] = defaultdict(int)
    for terms in documents:
        for term in {term for term, _ in terms}:
            document_frequency[term] += 1

    vocabulary = sorted(term for term, df in document_frequency.items() if df >= min_df)
    index = {term: i for i, term in enumerate(vocabulary)}

    n_docs = len(documents)
    matrix = np.zeros((n_docs, len(vocabulary)), dtype=np.float32)
    if not vocabulary:
        return TfidfMatrix(matrix, vocabulary)

    for row, terms in enumerate(documents):
        for term, weight in terms:
            col = index.get(term)
            if col is not None:
                matrix[row, col] += weight

    # 빈도가 높은 단어의 영향을 눌러주는 sublinear TF.
    np.log1p(matrix, out=matrix)

    df = np.array([document_frequency[term] for term in vocabulary], dtype=np.float32)
    idf = np.log((1.0 + n_docs) / (1.0 + df)) + 1.0
    matrix *= idf

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    np.divide(matrix, norms, out=matrix, where=norms > 0)

    return TfidfMatrix(matrix, vocabulary)
