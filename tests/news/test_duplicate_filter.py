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
