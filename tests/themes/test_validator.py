from __future__ import annotations

from pipelines.themes.transformers.validator import (
    _find_duplicate_name,
    _is_name_contained,
    _normalize_name,
    _overlap_coefficient,
)


def test_normalize_name_strips_all_whitespace() -> None:
    assert _normalize_name("2차 전지") == _normalize_name("2차전지")


def test_overlap_coefficient_empty_sets() -> None:
    assert _overlap_coefficient(set(), {"005930"}) == 0.0
    assert _overlap_coefficient({"005930"}, set()) == 0.0


def test_overlap_coefficient_uses_smaller_set_size() -> None:
    a = {"005930", "000660"}
    b = {"005930", "000660", "035420", "035720"}
    assert _overlap_coefficient(a, b) == 1.0


def test_is_name_contained_respects_min_length() -> None:
    assert _is_name_contained("반도체", "반도체소재") is True
    assert _is_name_contained("가", "가나") is False


def test_find_duplicate_name_matches_exact_normalized_name() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="2차전지", source="naver")
    existing = {"2차 전지": {"005930"}}

    assert _find_duplicate_name(theme, set(), existing) == "2차 전지"


def test_find_duplicate_name_returns_none_below_containment_floor() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="반도체소재", source="naver")
    candidate_stocks = {"005930", "000660"}
    existing = {"반도체": {"005930", "000660", "035420", "035720", "051910", "005380"}}

    assert _find_duplicate_name(theme, candidate_stocks, existing) is None
