"""계약상대 역매칭 단위 테스트. 스텁 마스터만 쓰고 DB·네트워크는 없다."""

from __future__ import annotations

from pipelines.disclosures.models import CorpMasterRow
from pipelines.disclosures.transformers.counterparty import (
    CorpMaster,
    CorpResolver,
    candidates,
    is_multi_company,
    looks_anonymous,
    norm,
)

MASTER = [
    CorpMasterRow(company_id=1, corp_code="00164788", name="현대모비스", ticker="012330"),
    CorpMasterRow(company_id=2, corp_code="00126380", name="삼성전자", ticker="005930"),
    CorpMasterRow(company_id=3, corp_code="01133217", name="LG CNS", ticker=None),
    CorpMasterRow(company_id=4, corp_code="00200001", name="한국가스공사", ticker="036460"),
    # 같은 이름의 다른 두 법인 — 모호하므로 매칭을 거부해야 한다.
    CorpMasterRow(company_id=5, corp_code="00300001", name="대성산업", ticker="128820"),
    CorpMasterRow(company_id=6, corp_code="00300002", name="대성산업", ticker=None),
]


def make_resolver() -> CorpResolver:
    return CorpResolver(MASTER)


def test_norm_strips_legal_forms_and_symbols() -> None:
    assert norm("현대모비스(주)") == norm("주식회사 현대모비스") == norm("현대모비스")
    assert norm("Samsung Co., Ltd.") == norm("samsung")


def test_resolves_exact_and_legal_form_variants() -> None:
    resolver = make_resolver()
    for spelling in ("현대모비스", "현대모비스(주)", "㈜현대모비스", "주식회사 현대모비스"):
        result = resolver.resolve(spelling)
        assert result["counterparty_corp_code"] == "00164788"
        assert result["counterparty_ticker"] == "012330"
        assert result["counterparty_company_id"] == 1
        assert result["counterparty_match_rule"] == "original:listed"


def test_curated_alias_substitution() -> None:
    # 통용 한글표기는 sync_dart_corp_codes 가 시드한 CURATED 별칭(DB)으로 해석된다.
    resolver = CorpResolver(MASTER, db_aliases=[("엘지씨엔에스", 3)])
    result = resolver.resolve("엘지씨엔에스")
    assert result["counterparty_corp_code"] == "01133217"
    assert result["counterparty_ticker"] is None
    assert result["counterparty_match_rule"] == "original:db_alias"


def test_bilingual_parenthesis_split() -> None:
    resolver = make_resolver()
    result = resolver.resolve("한국가스공사(KOREA GAS CORPORATION)")
    assert result["counterparty_corp_code"] == "00200001"
    assert result["counterparty_match_rule"].startswith("paren_outer")


def test_ambiguous_name_refuses_to_guess() -> None:
    # 상장사를 우선하면 틀린 추측이 자신 있어 보일 뿐이다 — 전부 None 이어야 한다.
    result = make_resolver().resolve("대성산업")
    assert result["counterparty_corp_code"] is None
    assert result["counterparty_match_rule"] is None


def test_anonymous_and_multi_company_short_circuit() -> None:
    resolver = make_resolver()
    assert looks_anonymous("국내 반도체 기업")
    assert looks_anonymous("비공개")
    assert not looks_anonymous("한국가스공사")
    assert is_multi_company("삼성전자(주), 현대모비스(주)")
    assert resolver.resolve("국내 반도체 기업")["counterparty_corp_code"] is None
    assert resolver.resolve("삼성전자(주), 현대모비스(주)")["counterparty_corp_code"] is None


def test_none_and_unknown_return_empty() -> None:
    resolver = make_resolver()
    assert resolver.resolve(None)["counterparty_corp_code"] is None
    assert resolver.resolve("존재하지않는회사")["counterparty_corp_code"] is None


def test_candidates_order() -> None:
    forms = candidates("한국가스공사(KOREA GAS CORPORATION)")
    assert forms[0][0] == "original"
    assert ("paren_outer", "한국가스공사") in forms
    assert ("paren_inner", "KOREA GAS CORPORATION") in forms


def test_memoization_returns_copies() -> None:
    resolver = make_resolver()
    first = resolver.resolve("현대모비스")
    first["counterparty_corp_code"] = "변조"
    assert resolver.resolve("현대모비스")["counterparty_corp_code"] == "00164788"


# ── DART 법인명 별칭 ──────────────────────────────────────────────────────────
#
# 상장사의 companies.name은 KIS 종목명("현대차")으로 덮여 DART 법인명("현대자동차")이
# 마스터에서 사라진다. company_aliases(source='DART')로 보존한 법인명을 리졸버가
# 함께 찾아야 한다.

DART_MASTER = MASTER + [
    CorpMasterRow(company_id=7, corp_code="00164742", name="현대차", ticker="005380"),
]

DART_ALIASES = [
    ("현대자동차", 7),
]


def test_db_alias_resolves_official_corp_name() -> None:
    resolver = CorpResolver(DART_MASTER, db_aliases=DART_ALIASES)
    for spelling in ("현대자동차", "현대자동차(주)", "주식회사 현대자동차"):
        result = resolver.resolve(spelling)
        assert result["counterparty_corp_code"] == "00164742"
        assert result["counterparty_ticker"] == "005380"
        assert result["counterparty_company_id"] == 7
        assert result["counterparty_match_rule"] == "original:db_alias"


def test_master_name_still_wins_over_alias_index() -> None:
    # 별칭이 있어도 마스터명 매칭 경로와 rule 표기는 그대로여야 한다.
    resolver = CorpResolver(DART_MASTER, db_aliases=DART_ALIASES)
    result = resolver.resolve("현대차")
    assert result["counterparty_corp_code"] == "00164742"
    assert result["counterparty_match_rule"] == "original:listed"


def test_alias_colliding_with_other_corp_refuses() -> None:
    # 별칭이 다른 법인의 마스터명과 같은 키가 되면 — 합친 키 공간에서 모호하므로
    # 마스터명 쪽도 별칭 쪽도 매칭하지 않는다. 틀린 식별자는 null보다 나쁘다.
    resolver = CorpResolver(DART_MASTER, db_aliases=[("삼성전자", 7)])
    assert resolver.resolve("삼성전자")["counterparty_corp_code"] is None


def test_alias_colliding_with_other_alias_refuses() -> None:
    resolver = CorpResolver(DART_MASTER, db_aliases=[("현대자동차", 7), ("현대자동차", 2)])
    assert resolver.resolve("현대자동차")["counterparty_corp_code"] is None


def test_alias_matching_own_master_name_is_not_ambiguous() -> None:
    # 같은 법인을 가리키는 별칭과 마스터명은 중복이지 모호가 아니다.
    resolver = CorpResolver(DART_MASTER, db_aliases=[("현대차", 7)])
    result = resolver.resolve("현대차")
    assert result["counterparty_corp_code"] == "00164742"


def test_alias_to_unknown_company_id_is_ignored() -> None:
    resolver = CorpResolver(DART_MASTER, db_aliases=[("유령회사", 999)])
    assert resolver.resolve("유령회사")["counterparty_corp_code"] is None


def test_corp_master_passes_aliases_to_resolver() -> None:
    master = CorpMaster(DART_MASTER, db_aliases=DART_ALIASES)
    result = master.resolve_counterparty("현대자동차(주)")
    assert result["counterparty_corp_code"] == "00164742"
    assert master.resolve_filer("00164742").ticker == "005380"
