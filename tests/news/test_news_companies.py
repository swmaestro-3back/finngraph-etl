"""news_companies 연결 로직 단위 테스트. DB 조회·INSERT 는 갈아끼운다."""

from __future__ import annotations

from pipelines.news.repositories import news_companies


def test_link_saved_items_resolves_names_and_falls_back_to_query_company(monkeypatch, caplog):
    monkeypatch.setattr(
        news_companies, "fetch_company_ids_by_stock_names", lambda names: {"에코프로": 200}
    )
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
            "_query_companies": [{"company_id": 100, "name": "엘앤에프"}],
            "_subject_names": ["엘앤에프", "에코프로", "모르는회사"],
        },
        # 기존 행 스킵은 첫 처리 때 연결이 끝났으므로 건드리지 않는다
        {"_news_id": 2, "_save_action": "skipped_existing", "_subject_names": ["에코프로"]},
    ]

    result = news_companies.link_saved_items(items)

    # 엘앤에프는 해석에 없지만 출처 종목이라 company_id 를 이미 안다
    assert inserted == [(1, [100, 200])]
    assert result == {"rows": 2, "unresolved_names": 1, "failed": 0}
    assert "모르는회사" in caplog.text


def test_link_saved_items_isolates_insert_failure(monkeypatch):
    monkeypatch.setattr(
        news_companies, "fetch_company_ids_by_stock_names", lambda names: {"에코프로": 200}
    )

    def failing_insert(news_id, ids):
        if news_id == 1:
            raise RuntimeError("db down")
        return len(ids)

    monkeypatch.setattr(news_companies, "insert_news_companies", failing_insert)

    items = [
        {"_news_id": 1, "_save_action": "inserted", "_subject_names": ["에코프로"]},
        {"_news_id": 2, "_save_action": "inserted", "_subject_names": ["에코프로"]},
    ]

    assert news_companies.link_saved_items(items) == {"rows": 1, "unresolved_names": 0, "failed": 1}
