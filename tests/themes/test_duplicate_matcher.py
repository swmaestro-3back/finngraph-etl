from __future__ import annotations

from pipelines.themes.transformers.duplicate_matcher import (
    core_name,
    find_duplicate_name,
    generic_coverage,
    is_parenthetical_variant,
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


def testnormalize_name_ignores_case() -> None:
    assert normalize_name("퓨리오사ai") == normalize_name("퓨리오사AI")


def testcore_name_drops_parenthesis() -> None:
    assert core_name("가상화폐(비트코인 등)") == "가상화폐"
    assert core_name("탈 플라스틱(친환경/생분해성 등)") == core_name("탈플라스틱")
    assert core_name("피팅(관이음쇠)/밸브") == core_name("피팅/밸브")


def testgeneric_coverage_is_measured_against_generic_side() -> None:
    generic = {"A", "B", "C", "D"}
    detailed = {"A", "B"}
    # 작은 쪽 기준이면 1.0 이지만, 일반 테마 기준으로는 절반만 덮는다
    assert overlap_coefficient(generic, detailed) == 1.0
    assert generic_coverage(generic, detailed) == 0.5


def testis_parenthetical_variant_merges_descriptive_parenthesis() -> None:
    generic = {"A", "B", "C", "D", "E"}
    detailed = {"A", "B", "C", "D", "X", "Y"}

    assert is_parenthetical_variant("가상화폐", generic, "가상화폐(비트코인 등)", detailed)
    assert is_parenthetical_variant("가상화폐(비트코인 등)", detailed, "가상화폐", generic)


def testis_parenthetical_variant_rejects_sub_category() -> None:
    generic = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
    sub = {"A", "B", "C"}

    assert not is_parenthetical_variant("2차전지", generic, "2차전지(전고체)", sub)


def testis_parenthetical_variant_rejects_both_parenthesized() -> None:
    stocks = {"A", "B", "C", "D", "E"}

    assert not is_parenthetical_variant("원자재(구리)", stocks, "원자재(리튬)", stocks)


def testis_parenthetical_variant_rejects_mere_containment() -> None:
    stocks = {"A", "B", "C", "D", "E"}

    assert not is_parenthetical_variant("로봇", stocks, "피지컬AI 로봇", stocks)
    assert not is_parenthetical_variant("보험", stocks, "손해보험", stocks)


def testfind_duplicate_name_matches_exact_normalized_name() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="2차전지", source="naver")
    existing = {"2차 전지": {"005930"}}

    assert find_duplicate_name(theme, set(), existing) == "2차 전지"


def testfind_duplicate_name_ignores_containment_without_parenthesis() -> None:
    from pipelines.themes.models import Theme

    stocks = {"005930", "000660", "035420", "035720", "051910"}
    theme = Theme(name="반도체소재", source="naver")
    existing = {"반도체": stocks}

    assert find_duplicate_name(theme, stocks, existing) is None


def testfind_duplicate_name_matches_parenthetical_variant() -> None:
    from pipelines.themes.models import Theme

    theme = Theme(name="메타버스(Metaverse)", source="naver")
    candidate_stocks = {"005930", "000660", "035420", "035720", "051910", "005380"}
    existing = {"메타버스": {"005930", "000660", "035420", "035720", "051910"}}

    assert find_duplicate_name(theme, candidate_stocks, existing) == "메타버스"
