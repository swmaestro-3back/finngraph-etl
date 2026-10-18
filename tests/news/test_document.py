"""문서 표현(document.py) 단위 테스트 — 서브벡터 3개 저장, all 은 유도. Kiwi 는 실제로 돈다."""

from __future__ import annotations

from pipelines.news.transformers.clustering.document import (
    FIELDS,
    STORED_FIELDS,
    build_term_weights,
    sum_terms,
    with_all,
)


def test_field_names():
    assert FIELDS == ("title", "body", "all", "entities")
    assert STORED_FIELDS == ("title", "body", "entities")


def test_sum_terms_folds_duplicates_across_lists():
    assert sum_terms([("a", 1.0), ("b", 2.0)], [("a", 0.5)]) == {"a": 1.5, "b": 2.0}
    assert sum_terms() == {}


def test_build_term_weights_has_stored_fields_only():
    weights = build_term_weights(
        "LG에너지솔루션 유상증자 결정", "LG에너지솔루션이 유상증자를 결정했다.", [12, 7]
    )

    assert set(weights) == set(STORED_FIELDS)
    # 복합명사 복원은 preprocess 그대로: 가중치 3.0
    assert weights["title"]["lg에너지솔루션"] == 3.0
    assert weights["body"]["lg에너지솔루션"] == 3.0
    # 개체는 company_id 문자열 키, 가중치 1.0 (jsonb 키는 문자열이다)
    assert weights["entities"] == {"12": 1.0, "7": 1.0}


def test_build_term_weights_with_empty_body_and_no_companies():
    weights = build_term_weights("삼성전자 실적", "", [])
    assert weights["body"] == {} and weights["entities"] == {}
    assert "삼성전자" in weights["title"]


def test_with_all_adds_title_plus_body_without_mutating_input():
    weights = {"title": {"a": 1.0, "b": 2.0}, "body": {"b": 1.0, "c": 4.0}, "entities": {"1": 1.0}}

    full = with_all(weights)

    assert set(full) == set(FIELDS)
    assert full["all"] == {"a": 1.0, "b": 3.0, "c": 4.0}
    assert full["entities"] == {"1": 1.0}
    assert "all" not in weights  # 원본은 그대로


def test_with_all_tolerates_missing_fields():
    # 이관 기사처럼 entities 가 없거나 필드가 빠진 문서도 빈 dict 로 채워진다
    assert with_all({}) == {"title": {}, "body": {}, "entities": {}, "all": {}}
    assert with_all({"title": {"a": 1.0}})["all"] == {"a": 1.0}
