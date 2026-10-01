"""검색 종목명 존재 필터 단위 테스트. DB·LLM 불필요."""

from __future__ import annotations

from pipelines.news.transformers.filters.company_mention_filter import (
    drop_query_company_unmentioned,
)


def test_drop_unmentioned_keeps_only_items_naming_the_query_company():
    query = {"company_id": 100, "name": "엘앤에프"}
    in_title = {"title": "엘앤에프, 양극재 공급", "description": "", "_query_company": query}
    in_snippet = {"title": "양극재 공급 계약", "description": "엘앤에프가", "_query_company": query}
    absent = {"title": "2차전지株 강세", "description": "에코프로 급등", "_query_company": query}
    no_query = {"title": "엘앤에프 강세", "description": ""}

    kept, removed = drop_query_company_unmentioned([in_title, in_snippet, absent, no_query])

    assert kept == [in_title, in_snippet]
    assert removed == [absent, no_query]


def test_drop_unmentioned_ignores_latin_case():
    item = {
        "title": "Naver, AI 투자 확대",
        "description": "",
        "_query_company": {"company_id": 1, "name": "NAVER"},
    }

    assert drop_query_company_unmentioned([item]) == ([item], [])
