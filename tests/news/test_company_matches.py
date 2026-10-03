"""제목 기업 매치 단위 테스트. 개체 사전은 인라인 매처로 대신한다."""

from __future__ import annotations

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.news.transformers import company_matches
from pipelines.news.transformers.company_matches import match_title_companies


def _entry(company_id: int, canonical: str) -> GazetteerEntry:
    return GazetteerEntry(
        company_id=company_id,
        stock_id=company_id + 1000,
        ticker=str(company_id),
        canonical=canonical,
    )


BOBCAT = _entry(100, "두산밥캣")
LGES = _entry(300, "LG에너지솔루션")
HYUNDAI = _entry(400, "현대차")
HYNIX = _entry(500, "SK하이닉스")
SK = _entry(600, "SK")

MATCHER = CompanyMatcher(
    {
        "두산밥캣": BOBCAT,
        "LG에너지솔루션": LGES,
        "LG엔솔": LGES,
        "현대차": HYUNDAI,
        "현대자동차": HYUNDAI,
        "SK하이닉스": HYNIX,
        "SK": SK,
    }
)


def _item(title: str, description: str = "", company_id: int = 100, name: str = "두산밥캣"):
    return {
        "title": title,
        "description": description,
        "_query_company": {"company_id": company_id, "name": name},
    }


def test_title_companies_put_query_company_first_and_dedupe():
    item = _item("LG엔솔·두산밥캣·현대차 상생 협약, LG에너지솔루션 주도")

    result = match_title_companies([item], MATCHER)

    assert result.kept == [item]
    # 검색 종목이 첫 번째, 나머지는 등장 순서, 같은 기업은 처음 표기 하나
    assert item["_title_companies"] == [
        {"company_id": 100, "name": "두산밥캣", "surface": "두산밥캣"},
        {"company_id": 300, "name": "LG에너지솔루션", "surface": "LG엔솔"},
        {"company_id": 400, "name": "현대차", "surface": "현대차"},
    ]
    assert (result.companies, result.max_companies) == (3, 3)


def test_query_company_only_in_snippet_is_removed():
    # 스니펫은 보지 않는다 — 제목에 검색 종목이 없으면 버린다
    item = _item("LG엔솔, 美 공장 증설", "두산밥캣도 협력사로 참여")

    result = match_title_companies([item], MATCHER)

    assert result.kept == []
    assert result.removed == [item]


def test_snippet_companies_are_not_title_companies():
    item = _item("두산밥캣, 북미 공장 증설", "LG에너지솔루션과 현대차도 참여")

    match_title_companies([item], MATCHER)

    assert [c["company_id"] for c in item["_title_companies"]] == [100]


def test_query_company_written_only_as_alias_is_kept_with_title_surface():
    item = _item("LG엔솔, 美 공장 증설", company_id=300, name="LG에너지솔루션")

    result = match_title_companies([item], MATCHER)

    assert result.kept == [item]
    assert item["_title_companies"] == [
        {"company_id": 300, "name": "LG에너지솔루션", "surface": "LG엔솔"}
    ]


def test_query_name_inside_longer_listed_name_is_not_a_mention():
    # 최장 일치라 'SK하이닉스' 안의 'SK' 는 SK 로 잡히지 않는다
    item = _item("SK하이닉스, HBM 공급", company_id=600, name="SK")

    result = match_title_companies([item], MATCHER)

    assert result.removed == [item]


def test_match_inside_longer_unknown_name_is_kept_for_llm_to_judge():
    # 사전에 없는 '두산밥캣코리아' 안의 '두산밥캣'도 잡힌다. 걸러내는 것은 LLM 의 몫이다.
    item = _item("현대차, 두산밥캣코리아와 협력", company_id=400, name="현대차")

    match_title_companies([item], MATCHER)

    assert [c["surface"] for c in item["_title_companies"]] == ["현대차", "두산밥캣"]


def test_article_without_query_company_is_removed():
    item = {"title": "두산밥캣 강세", "description": ""}

    assert match_title_companies([item], MATCHER).removed == [item]


def test_empty_items_do_not_load_gazetteer(monkeypatch):
    def fail():
        raise AssertionError("개체 사전을 읽음")

    monkeypatch.setattr(company_matches, "get_company_matcher", fail)

    result = match_title_companies([])

    assert (result.kept, result.removed, result.companies) == ([], [], 0)


def test_default_matcher_is_loaded_lazily(monkeypatch):
    monkeypatch.setattr(company_matches, "get_company_matcher", lambda: MATCHER)
    item = _item("두산밥캣, LG엔솔에 부품 공급")

    match_title_companies([item])

    assert [c["company_id"] for c in item["_title_companies"]] == [100, 300]
