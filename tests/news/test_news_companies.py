"""news_companies 연결 대상 선정 단위 테스트. DB 는 쓰지 않는다."""

from __future__ import annotations

from pipelines.news.repositories.postgres.news_companies import linked_company_ids


def test_linked_company_ids_are_the_companies_that_passed_judgement():
    item = {
        "_query_company": {"company_id": 100, "name": "두산밥캣"},
        "_linked_companies": [
            {"company_id": 100, "name": "두산밥캣"},
            {"company_id": 300, "name": "LG에너지솔루션"},
        ],
    }

    assert linked_company_ids(item) == [100, 300]


def test_linked_company_ids_fall_back_to_query_company():
    # 관련성 필터를 거치지 않은 기사는 검색 대상 기업 하나다
    assert linked_company_ids({"_query_company": {"company_id": 100, "name": "엘앤에프"}}) == [100]


def test_linked_company_ids_are_empty_when_judgement_linked_nothing():
    item = {"_query_company": {"company_id": 100, "name": "엘앤에프"}, "_linked_companies": []}

    assert linked_company_ids(item) == []
