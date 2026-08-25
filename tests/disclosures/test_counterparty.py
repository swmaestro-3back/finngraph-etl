"""계약상대 역매칭 단위 테스트. 스텁 마스터만 쓰고 DB·네트워크는 없다."""

from __future__ import annotations

from pipelines.disclosures.models import CorpMasterRow
from pipelines.disclosures.transformers.counterparty import (
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


def test_alias_substitution() -> None:
    resolver = make_resolver()
    result = resolver.resolve("엘지씨엔에스")
    assert result["counterparty_corp_code"] == "01133217"
    assert result["counterparty_ticker"] is None
    assert result["counterparty_match_rule"] == "original+alias:all"


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
