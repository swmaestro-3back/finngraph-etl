"""배치 내 URL 중복 제거 — 먼저 걸린 종목의 기사만 남는지 검증."""

from __future__ import annotations


def _item(link: str, title: str, company_id: int, name: str) -> dict:
    return {
        "title": title,
        "link": link,
        "originallink": link,
        "_query_company": {"company_id": company_id, "name": name},
    }


def test_url_duplicate_keeps_first_query_company():
    from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url

    first = _item("https://a.com/1", "엘앤에프·에코프로 동반 급등", 100, "엘앤에프")
    second = _item("https://a.com/1?ref=x", "엘앤에프·에코프로 동반 급등", 200, "에코프로")
    third = _item("https://a.com/1/", "같은 기사", 100, "엘앤에프")

    unique, removed = remove_duplicate_by_url([first, second, third])

    assert unique == [first]
    assert len(removed) == 2
    assert first["_query_company"] == {"company_id": 100, "name": "엘앤에프"}


def test_url_duplicate_keeps_every_query_company_that_fetched_the_article():
    from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url

    first = _item("https://a.com/1", "삼성전자·SK하이닉스 HBM 공급", 100, "삼성전자")
    second = _item("https://a.com/1?ref=x", "삼성전자·SK하이닉스 HBM 공급", 200, "SK하이닉스")
    third = _item("https://a.com/1/", "같은 기사", 100, "삼성전자")

    unique, _ = remove_duplicate_by_url([first, second, third])

    # 남는 사본은 첫 번째지만, 이 기사를 가져온 검색 종목은 모두 기억한다(같은 기업은 한 번)
    assert unique == [first]
    assert first["_query_companies"] == [
        {"company_id": 100, "name": "삼성전자"},
        {"company_id": 200, "name": "SK하이닉스"},
    ]


def test_items_without_query_company_get_no_query_companies():
    from pipelines.news.transformers.filters.duplicate_filter import remove_duplicate_by_url

    item = {"title": "t", "link": "https://a.com/9", "originallink": "https://a.com/9"}

    unique, _ = remove_duplicate_by_url([item, dict(item)])

    assert unique == [item]
    assert "_query_companies" not in item
