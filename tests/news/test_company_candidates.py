"""gazetteer(KRX 사전) 후보 추출 단위 테스트. DB·LLM 불필요."""

from __future__ import annotations


def test_extract_returns_canonical_names_in_order_without_duplicates():
    from pipelines.news.transformers.company_candidates import extract_company_candidates

    names = extract_company_candidates("삼전·엘앤에프 동반 급등, 엘앤에프는 LFP 기대…삼성전자도")

    assert names == ["삼성전자", "엘앤에프"]  # "삼전" 은 삼성전자의 표면형


def test_extract_ignores_text_without_companies():
    from pipelines.news.transformers.company_candidates import extract_company_candidates

    assert extract_company_candidates("코스피 7000선 사수…외국인 순매도") == []


def test_attach_puts_query_companies_first_and_merges_gazetteer():
    from pipelines.news.transformers.company_candidates import attach_candidate_companies

    items = [
        {
            "title": "에코프로, 양극재 증설…엘앤에프도 강세",
            "description": "삼성SDI 공급 기대",
            "_query_companies": [{"company_id": 100, "name": "엘앤에프"}],
        },
        {"title": "코스피 마감", "description": "", "_query_companies": []},
    ]

    attach_candidate_companies(items)

    assert items[0]["_candidate_companies"] == ["엘앤에프", "에코프로", "삼성SDI"]
    assert items[1]["_candidate_companies"] == []
