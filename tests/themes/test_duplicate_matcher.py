from __future__ import annotations

from pipelines.themes.transformers.duplicate_matcher import (
    find_duplicate_name,
    is_name_contained,
    normalize_name,
    overlap_coefficient,
)


def testnormalize_name_strips_all_whitespace() -> None:
    assert normalize_name("2차 전지") == normalize_name("2차전지")


def testnormalize_name_strips_special_characters() -> None:
    assert normalize_name("IT(소프트웨어/SI)") == normalize_name("IT 소프트웨어 SI")
    assert normalize_name("2차전지(소재)") == normalize_name("2차전지 소재")
    assert normalize_name("바이오-헬스케어") == normalize_name("바이오헬스케어")


def testoverlap_coefficient_empty_sets() -> None:
    assert overlap_coefficient(set(), {"005930"}) == 0.0
    assert overlap_coefficient({"005930"}, set()) == 0.0


def testoverlap_coefficient_uses_smaller_set_size() -> None:
    a = {"005930", "000660"}
    b = {"005930", "000660", "035420", "035720"}
    assert overlap_coefficient(a, b) == 1.0


def testis_name_contained_respects_min_length() -> None:
    assert is_name_contained("반도체", "반도체소재") is True
    assert is_name_contained("가", "가나") is False


def testfind_duplicate_name_matches_exact_normalized_name() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="2차전지", source="naver")
    existing = {"2차 전지": {"005930"}}

    assert find_duplicate_name(theme, set(), existing) == "2차 전지"


def testfind_duplicate_name_returns_none_below_containment_floor() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="반도체소재", source="naver")
    candidate_stocks = {"005930", "000660"}
    existing = {"반도체": {"005930", "000660", "035420", "035720", "051910", "005380"}}

    assert find_duplicate_name(theme, candidate_stocks, existing) is None
