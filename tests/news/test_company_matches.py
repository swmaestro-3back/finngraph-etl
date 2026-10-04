"""제목 기업 매치 단위 테스트. 개체 사전은 인라인 매처로 대신한다."""

from __future__ import annotations

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.news.transformers import company_matches
from pipelines.news.transformers.company_matches import match_body_companies, match_title_companies


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


def _listing(title: str) -> int:
    item = _item(title)
    match_title_companies([item], MATCHER)
    return item["_title_listing"]


def test_title_listing_counts_companies_joined_by_separators():
    # 가운뎃점·쉼표·빗금만 사이에 두고 이어진 기업 수
    assert _listing("LG엔솔·두산밥캣·현대차 상생 협약") == 3
    assert _listing("두산밥캣ㆍLG엔솔ㆍ현대차ㆍSK하이닉스 등") == 4
    assert _listing("두산밥캣, LG엔솔, 현대차, SK하이닉스...") == 4
    assert _listing("두산밥캣, LG엔솔·현대차와 배터리 공급 계약") == 3


def test_title_listing_is_broken_by_words_between_companies():
    # 사이에 낱말이 끼면 나열이 아니다 — 가장 긴 나열만 센다
    assert _listing("두산밥캣 급등…LG엔솔·현대차도 강세") == 2
    assert _listing("두산밥캣, 3분기 실적 발표") == 1
    assert _listing("두산밥캣과 현대차, SK하이닉스에 납품") == 2


def test_title_listing_counts_a_company_once():
    # 같은 기업의 다른 표기는 한 기업이다
    assert _listing("두산밥캣·LG엔솔·LG에너지솔루션 협약") == 2


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


# ── 본문 기업 매치 ───────────────────────────────────────────────────────────


def _body_item(text: str, title_companies: list[dict] | None = None) -> dict:
    item = {
        "title": "두산밥캣, 북미 공장 증설",
        "_text": text,
        "_query_company": {"company_id": 100, "name": "두산밥캣"},
    }
    if title_companies is not None:
        item["_title_companies"] = title_companies
    return item


def test_body_companies_exclude_companies_judged_from_the_title():
    # 현대차는 제목에서 판정받았다(통과했든 탈락했든). 본문에 다시 나와도 후보가 아니다.
    item = _body_item(
        "두산밥캣은 LG엔솔 배터리를 쓴다. 현대차도 참여했다. SK하이닉스가 칩을 댄다.",
        [
            {"company_id": 100, "name": "두산밥캣", "surface": "두산밥캣"},
            {"company_id": 400, "name": "현대차", "surface": "현대차"},
        ],
    )

    count = match_body_companies([item], MATCHER)

    assert count == 2
    assert item["_body_companies"] == [
        {"company_id": 300, "name": "LG에너지솔루션", "surface": "LG엔솔"},
        {"company_id": 500, "name": "SK하이닉스", "surface": "SK하이닉스"},
    ]


def test_body_companies_keep_each_spelling_of_one_company_once():
    item = _body_item(
        "LG엔솔이 증설한다. LG에너지솔루션은 공급을 늘린다. LG엔솔은 투자한다.",
        [{"company_id": 100, "name": "두산밥캣", "surface": "두산밥캣"}],
    )

    match_body_companies([item], MATCHER)

    # 같은 기업의 다른 표기는 따로 판정받는다. 같은 표기는 한 번만 든다.
    assert [c["surface"] for c in item["_body_companies"]] == ["LG엔솔", "LG에너지솔루션"]


def test_body_companies_exclude_query_company_without_title_companies():
    # 제목 매치를 거치지 않은 기사도 검색 종목은 관련성 필터가 판정했다
    item = _body_item("두산밥캣이 현대차와 손잡았다.")

    match_body_companies([item], MATCHER)

    assert [c["company_id"] for c in item["_body_companies"]] == [400]


def test_body_companies_are_empty_without_body():
    item = _body_item("", [{"company_id": 100, "name": "두산밥캣", "surface": "두산밥캣"}])
    del item["_text"]

    assert match_body_companies([item], MATCHER) == 0
    assert item["_body_companies"] == []


def test_body_match_with_empty_items_does_not_load_gazetteer(monkeypatch):
    def fail():
        raise AssertionError("개체 사전을 읽음")

    monkeypatch.setattr(company_matches, "get_company_matcher", fail)

    assert match_body_companies([]) == 0
