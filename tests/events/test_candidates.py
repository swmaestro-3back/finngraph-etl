"""후보 정규명 추출. DB 대신 인라인 사전으로 만든 공통 매처(CompanyMatcher)를 쓴다."""

from __future__ import annotations

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.events.transformers.candidates import extract_company_candidates


def _entry(company_id: int, ticker: str, canonical: str) -> GazetteerEntry:
    return GazetteerEntry(
        company_id=company_id, stock_id=company_id, ticker=ticker, canonical=canonical
    )


SAMSUNG = _entry(1, "005930", "삼성전자")
KIA = _entry(2, "000270", "기아")
ALPHABET = _entry(3, "GOOGL", "알파벳A")

# 알파벳A 는 정식명을 별칭으로 넣지 않았다 — 원문에서 후보를 뽑는다는 걸 확인하는 경우
GAZETTEER = {
    "삼성전자": SAMSUNG,
    "삼전": SAMSUNG,
    "기아": KIA,
    "기아차": KIA,
    "구글": ALPHABET,
}

MATCHER = CompanyMatcher(GAZETTEER)


def test_canonicalizes_text_and_returns_canonical_candidates():
    texts = ["삼전 주가 급등\n삼전이 유증을 결의했다"]

    canonical_texts, candidates = extract_company_candidates(texts, MATCHER)

    assert canonical_texts == ["삼성전자 주가 급등\n삼성전자이 유증을 결의했다"]
    assert candidates == ["삼성전자"]


def test_candidates_are_deduped_in_first_appearance_order_across_members():
    texts = [
        "기아차 리콜\n기아차가 리콜한다. 삼성전자 언급",
        "삼전 실적\n삼성전자 실적 발표. 기아도 언급",
    ]

    _, candidates = extract_company_candidates(texts, MATCHER)

    assert candidates == ["기아", "삼성전자"]


def test_candidate_found_even_when_canonical_is_not_an_alias():
    # 후보는 원문에서 뽑는다 — 정규화된 텍스트 "알파벳A" 에는 별칭이 없어 못 찾기 때문
    texts = ["구글 실적\n구글이 실적을 냈다"]

    canonical_texts, candidates = extract_company_candidates(texts, MATCHER)

    assert canonical_texts == ["알파벳A 실적\n알파벳A이 실적을 냈다"]
    assert candidates == ["알파벳A"]


def test_no_candidates():
    canonical_texts, candidates = extract_company_candidates(
        ["코스피 2700 회복\n외국인 순매수"], MATCHER
    )

    assert canonical_texts == ["코스피 2700 회복\n외국인 순매수"]
    assert candidates == []


def test_empty_input():
    assert extract_company_candidates([], MATCHER) == ([], [])
