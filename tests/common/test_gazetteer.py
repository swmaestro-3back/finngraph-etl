"""개체 사전 공통 매처 단위 테스트 (DB 없이 인라인 사전)."""

from __future__ import annotations

import pytest

from pipelines.common import gazetteer
from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry

SAMSUNG = GazetteerEntry(company_id=1, stock_id=11, ticker="005930", canonical="삼성전자")
KIA = GazetteerEntry(company_id=2, stock_id=12, ticker="000270", canonical="기아")
META = GazetteerEntry(company_id=3, stock_id=13, ticker="META", canonical="메타플랫폼스")

ENTRIES = {
    "삼성전자": SAMSUNG,
    "삼전": SAMSUNG,
    "기아": KIA,
    "기아차": KIA,
    "메타플랫폼스": META,
    "메타 플랫폼스": META,
}


@pytest.fixture(autouse=True)
def _fresh_cache():
    gazetteer.reset_company_matcher()
    yield
    gazetteer.reset_company_matcher()


@pytest.fixture
def matcher() -> CompanyMatcher:
    return CompanyMatcher(ENTRIES)


def test_extract_returns_article_spelling_span_and_ids(matcher):
    text = "삼전은 메타 플랫폼스에 공급한다"

    matches = matcher.extract(text)

    assert [(m.text, m.canonical, m.entry.company_id) for m in matches] == [
        ("삼전", "삼성전자", 1),
        ("메타 플랫폼스", "메타플랫폼스", 3),
    ]
    assert [text[m.start : m.end] for m in matches] == ["삼전", "메타 플랫폼스"]
    assert matches[1].entry.ticker == "META"
    assert matches[1].entry.stock_id == 13


def test_extract_prefers_longest_alias(matcher):
    assert [m.text for m in matcher.extract("기아차 리콜")] == ["기아차"]


def test_canonicalize_replaces_aliases_with_canonical_names(matcher):
    assert (
        matcher.canonicalize("삼전과 기아차, 그리고 삼성전자") == "삼성전자과 기아, 그리고 삼성전자"
    )


def test_canonicalize_without_matches_returns_text(matcher):
    assert matcher.canonicalize("코스피 2700 회복") == "코스피 2700 회복"


def test_get_company_matcher_raises_on_empty_gazetteer(monkeypatch):
    monkeypatch.setattr(gazetteer, "load_gazetteer_entries", lambda session: {})

    with pytest.raises(RuntimeError, match="entity_gazetteer"):
        gazetteer.get_company_matcher()


def test_get_company_matcher_is_cached(monkeypatch):
    calls = []

    def load(session):
        calls.append(1)
        return ENTRIES

    monkeypatch.setattr(gazetteer, "load_gazetteer_entries", load)

    assert gazetteer.get_company_matcher() is gazetteer.get_company_matcher()
    assert len(calls) == 1
