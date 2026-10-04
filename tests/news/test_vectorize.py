"""전역 IDF 벡터화 단위 테스트 (DB/외부 인프라 불필요)."""

from __future__ import annotations

import math

import pytest

from pipelines.news.transformers.clustering.vectorize import IdfTable, cosine, vectorize


def test_idf_is_one_when_table_is_empty():
    # 첫 실행에는 저장된 기사가 없다 — 모든 토큰이 같은 무게다
    table = IdfTable()

    assert table.idf("삼성전자") == pytest.approx(1.0)


def test_rare_token_gets_higher_idf_than_common_token():
    table = IdfTable(document_frequency={"삼성전자": 90, "해상변전소": 2}, document_count=100)

    assert table.idf("해상변전소") > table.idf("삼성전자")
    # 처음 보는 토큰은 df=0 으로 보고 가장 큰 값을 준다
    assert table.idf("처음보는토큰") > table.idf("해상변전소")
    assert table.idf("처음보는토큰") == pytest.approx(math.log(101.0) + 1.0)


def test_vectorize_is_l2_normalized_and_sums_duplicate_tokens():
    vector = vectorize([("엘앤에프", 2.0), ("양극재", 1.0), ("엘앤에프", 1.0)], IdfTable())

    assert set(vector) == {"엘앤에프", "양극재"}
    assert math.sqrt(sum(value * value for value in vector.values())) == pytest.approx(1.0)
    # 같은 토큰은 가중치를 더한 뒤 log1p 를 건다: log1p(3) : log1p(1)
    assert vector["엘앤에프"] / vector["양극재"] == pytest.approx(math.log1p(3.0) / math.log1p(1.0))


def test_vectorize_without_terms_is_empty():
    assert vectorize([], IdfTable()) == {}
    assert vectorize([("엘앤에프", 0.0)], IdfTable()) == {}


def test_vectorize_weights_rare_tokens_by_the_fixed_table():
    # 표가 고정이라 같은 문서는 어떤 배치에 섞여도 같은 벡터다. 흔한 토큰은 눌리고 드문 토큰이 선다.
    table = IdfTable(document_frequency={"삼성전자": 90}, document_count=100)

    vector = vectorize([("삼성전자", 1.0), ("해상변전소", 1.0)], table)

    assert vector["해상변전소"] > vector["삼성전자"]
    # TF 가 같으므로 두 값의 비는 IDF 의 비다
    assert vector["해상변전소"] / vector["삼성전자"] == pytest.approx(
        table.idf("해상변전소") / table.idf("삼성전자")
    )


def test_cosine_of_identical_vectors_is_one_and_disjoint_is_zero():
    left = vectorize([("엘앤에프", 3.0), ("양극재", 1.0)], IdfTable())
    other = vectorize([("삼성전자", 3.0)], IdfTable())

    assert cosine(left, left) == pytest.approx(1.0)
    assert cosine(left, other) == 0.0
    assert cosine(left, {}) == 0.0
