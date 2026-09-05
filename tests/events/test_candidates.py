"""후보 정규명 추출. 비공개 gazetteer 대신 세 항목짜리 사전으로 만든 가짜 extractor 를 쓴다."""

from __future__ import annotations

from types import SimpleNamespace

from flashtext import KeywordProcessor

from pipelines.events.transformers.candidates import extract_company_candidates

GAZETTEER = {
    "삼성전자": ["삼성전자", "삼전"],
    "기아": ["기아", "기아차"],
    "알파벳 A": ["구글"],  # 정규명이 별칭 목록에 없는 경우 (US 사전에 실제로 있다)
}


class FakeExtractor:
    """EntityExtractor 와 같은 두 메서드. 반환 항목은 .text 만 가진다."""

    def __init__(self, gazetteer: dict[str, list[str]]):
        self._processor = KeywordProcessor(case_sensitive=True)
        self._processor.add_keywords_from_dict(gazetteer)

    def canonicalize(self, text: str) -> str:
        return self._processor.replace_keywords(text)

    def extract(self, text: str) -> list:
        return [SimpleNamespace(text=name) for name in self._processor.extract_keywords(text)]


def test_canonicalizes_text_and_returns_canonical_candidates():
    texts = ["삼전 주가 급등\n삼전이 유증을 결의했다"]

    canonical_texts, candidates = extract_company_candidates(texts, FakeExtractor(GAZETTEER))

    assert canonical_texts == ["삼성전자 주가 급등\n삼성전자이 유증을 결의했다"]
    assert candidates == ["삼성전자"]


def test_candidates_are_deduped_in_first_appearance_order_across_members():
    texts = [
        "기아차 리콜\n기아차가 리콜한다. 삼성전자 언급",
        "삼전 실적\n삼성전자 실적 발표. 기아도 언급",
    ]

    _, candidates = extract_company_candidates(texts, FakeExtractor(GAZETTEER))

    assert candidates == ["기아", "삼성전자"]


def test_candidate_found_even_when_canonical_is_not_an_alias():
    # 후보는 원문에서 뽑는다 — 정규화된 텍스트 "알파벳 A" 에는 별칭이 없어 못 찾기 때문
    texts = ["구글 실적\n구글이 실적을 냈다"]

    canonical_texts, candidates = extract_company_candidates(texts, FakeExtractor(GAZETTEER))

    assert canonical_texts == ["알파벳 A 실적\n알파벳 A이 실적을 냈다"]
    assert candidates == ["알파벳 A"]


def test_no_candidates():
    canonical_texts, candidates = extract_company_candidates(
        ["코스피 2700 회복\n외국인 순매수"], FakeExtractor(GAZETTEER)
    )

    assert canonical_texts == ["코스피 2700 회복\n외국인 순매수"]
    assert candidates == []


def test_empty_input():
    assert extract_company_candidates([], FakeExtractor(GAZETTEER)) == ([], [])
