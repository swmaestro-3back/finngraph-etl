"""news_companies 연결 로직 단위 테스트. INSERT 는 갈아끼운다."""

from __future__ import annotations

from pipelines.news.repositories import news_companies


def test_link_saved_items_links_query_company(monkeypatch):
    inserted: list[tuple[int, list[int]]] = []
    monkeypatch.setattr(
        news_companies,
        "insert_news_companies",
        lambda news_id, ids: inserted.append((news_id, ids)) or len(ids),
    )

    items = [
        {
            "_news_id": 1,
            "_save_action": "inserted",
            "_query_company": {"company_id": 100, "name": "엘앤에프"},
        },
        # 기존 행 스킵은 첫 처리 때 연결이 끝났으므로 건드리지 않는다
        {
            "_news_id": 2,
            "_save_action": "skipped_existing",
            "_query_company": {"company_id": 200, "name": "에코프로"},
        },
    ]

    result = news_companies.link_saved_items(items)

    assert inserted == [(1, [100])]
    assert result == {"rows": 1, "failed": 0}


def test_link_saved_items_isolates_insert_failure(monkeypatch):
    def failing_insert(news_id, ids):
        if news_id == 1:
            raise RuntimeError("db down")
        return len(ids)

    monkeypatch.setattr(news_companies, "insert_news_companies", failing_insert)

    company = {"company_id": 200, "name": "에코프로"}
    items = [
        {"_news_id": 1, "_save_action": "inserted", "_query_company": company},
        {"_news_id": 2, "_save_action": "inserted", "_query_company": company},
    ]

    assert news_companies.link_saved_items(items) == {"rows": 1, "failed": 1}
