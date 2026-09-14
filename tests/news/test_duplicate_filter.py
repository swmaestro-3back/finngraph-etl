"""배치 내 중복 제거가 검색 출처 종목(_query_companies)을 병합하는지 검증."""

from __future__ import annotations


def _item(link: str, title: str, companies: list[tuple[int, str]]) -> dict:
    return {
        "title": title,
        "link": link,
        "originallink": link,
        "_query_companies": [{"company_id": cid, "name": name} for cid, name in companies],
    }


def test_url_duplicate_merges_query_companies():
    from pipelines.news.transformers.duplicate_filter import remove_duplicate_by_url

    first = _item("https://a.com/1", "엘앤에프·에코프로 동반 급등", [(100, "엘앤에프")])
    second = _item("https://a.com/1?ref=x", "엘앤에프·에코프로 동반 급등", [(200, "에코프로")])
    third = _item("https://a.com/1/", "같은 기사", [(100, "엘앤에프")])

    unique, removed = remove_duplicate_by_url([first, second, third])

    assert unique == [first]
    assert len(removed) == 2
    assert first["_query_companies"] == [
        {"company_id": 100, "name": "엘앤에프"},
        {"company_id": 200, "name": "에코프로"},
    ]


def test_merge_without_query_companies_is_noop():
    from pipelines.news.transformers.duplicate_filter import merge_query_companies

    target = {"title": "a"}
    merge_query_companies(target, {"title": "b"})

    assert target["_query_companies"] == []
