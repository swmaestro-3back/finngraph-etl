from __future__ import annotations

from pipelines.themes.transformers.validator import (
    _find_duplicate_name,
    _is_name_contained,
    _normalize_name,
    _overlap_coefficient,
)


def test_normalize_name_strips_all_whitespace() -> None:
    assert _normalize_name("2차 전지") == _normalize_name("2차전지")


def test_normalize_name_strips_special_characters() -> None:
    assert _normalize_name("IT(소프트웨어/SI)") == _normalize_name("IT 소프트웨어 SI")
    assert _normalize_name("2차전지(소재)") == _normalize_name("2차전지 소재")
    assert _normalize_name("바이오-헬스케어") == _normalize_name("바이오헬스케어")


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


def test_merge_reason_joins_both_sentences() -> None:
    from pipelines.themes.transformers.validator import _merge_reason

    assert _merge_reason("배터리 셀 생산", "전기차용 중대형 배터리 매출 비중") == (
        "배터리 셀 생산\n전기차용 중대형 배터리 매출 비중"
    )


def test_merge_reason_takes_the_only_available_side() -> None:
    from pipelines.themes.transformers.validator import _merge_reason

    assert _merge_reason(None, "양극재 공급") == "양극재 공급"
    assert _merge_reason("양극재 공급", None) == "양극재 공급"
    assert _merge_reason("  ", "양극재 공급") == "양극재 공급"


def test_merge_reason_without_any_text_is_none() -> None:
    from pipelines.themes.transformers.validator import _merge_reason

    assert _merge_reason(None, None) is None
    assert _merge_reason("  ", None) is None


def test_validate_theme_merges_reasons_of_shared_stocks_only() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.validator import validate_theme

    kept = Theme(
        name="2차전지",
        source="judal",
        companies=[
            Company(name="삼성SDI", ticker="006400", reason="배터리 셀 생산"),
            Company(name="LG에너지솔루션", ticker="373220", reason=None),
        ],
    )
    # 정규화하면 이름이 같아 중복으로 걸린다
    duplicate = Theme(
        name="2차 전지",
        source="naver",
        companies=[
            Company(name="삼성SDI", ticker="006400", reason="전기차용 중대형 배터리 매출 비중"),
            Company(name="LG에너지솔루션", ticker="373220", reason="북미 합작 공장 증설"),
            Company(name="에코프로비엠", ticker="247540", reason="양극재 공급"),
        ],
    )

    [result] = validate_theme([kept, duplicate])

    assert result is kept
    reasons = {c.ticker: c.reason for c in result.companies}

    # 양쪽에 다 있는 종목은 두 문장이 이어붙는다
    assert reasons["006400"] == "배터리 셀 생산\n전기차용 중대형 배터리 매출 비중"
    # 채택된 쪽에 사유가 없었으면 중복 테마의 문장으로 채워진다
    assert reasons["373220"] == "북미 합작 공장 증설"
    # 중복 테마에만 있던 종목은 편입되지 않는다
    assert "247540" not in reasons


def test_validate_theme_keeps_first_source_order() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.validator import validate_theme

    first = Theme(
        name="반도체", source="judal", companies=[Company(name="삼성전자", ticker="005930")]
    )
    second = Theme(
        name="반도체", source="naver", companies=[Company(name="삼성전자", ticker="005930")]
    )

    [result] = validate_theme([first, second])

    # SOURCES 순서상 먼저 온 테마가 남는다
    assert result.source == "judal"
    # 버려진 테마의 소스는 채택된 테마에 쌓인다
    assert result.sources == ["judal", "naver"]


def test_validate_theme_does_not_repeat_the_same_source() -> None:
    from pipelines.themes.models import Company, Theme
    from pipelines.themes.transformers.validator import validate_theme

    stock = [Company(name="삼성전자", ticker="005930")]
    themes = [
        Theme(name="반도체", source="judal", companies=stock),
        Theme(name="반 도체", source="naver", companies=stock),
        Theme(name="반도체", source="naver", companies=stock),
    ]

    [result] = validate_theme(themes)

    assert result.sources == ["judal", "naver"]
