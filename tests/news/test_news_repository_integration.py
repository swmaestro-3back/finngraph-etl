"""news repository 통합 테스트 — 저장, 저장된 URL 대조, 요약 대상.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.news import (
    fetch_unsummarized_news_items,
    remove_stored_by_url,
    save_news_items,
)
from pipelines.triples.loaders.postgres import mark_triple_extraction_result

pytestmark = pytest.mark.integration

BODY = "엘앤에프가 삼성SDI 에 양극재를 공급하는 계약을 체결했다고 밝혔다. " * 5


def _item(link: str, title: str = "엘앤에프, 양극재 공급 계약") -> dict:
    return {
        "title": title,
        "description": "",
        "link": link,
        "originallink": link,
        "pubDate": "Wed, 09 Sep 2026 10:00:00 +0900",
        "_text": BODY,
    }


@pytest.fixture
def link():
    marker = uuid.uuid4().hex
    yield f"https://example.com/news/{marker}"
    with session_scope() as session:
        session.execute(text("DELETE FROM news WHERE link LIKE :prefix"), {"prefix": f"%{marker}%"})


def test_save_inserts_then_skips_same_link(link):
    first = _item(link)
    result = save_news_items([first], save_summary=False, skip_existing=True)

    assert result["inserted_count"] == 1
    assert first["_save_action"] == "inserted"
    news_id = first["_news_id"]

    again = _item(link)
    result = save_news_items([again], save_summary=False, skip_existing=True)

    assert result["skipped_existing_count"] == 1
    assert again["_save_action"] == "skipped_existing"
    assert again["_news_id"] == news_id


def test_save_treats_query_string_and_trailing_slash_as_same_article(link):
    save_news_items([_item(link)], save_summary=False, skip_existing=True)

    variant = _item(f"{link}/?utm_source=x")
    result = save_news_items([variant], save_summary=False, skip_existing=True)

    assert result["skipped_existing_count"] == 1


def test_save_skips_item_without_body(link):
    empty = _item(link)
    empty["_text"] = ""

    result = save_news_items([empty], save_summary=False, skip_existing=True)

    assert result["skipped_no_body_count"] == 1
    assert "_news_id" not in empty


def test_remove_stored_by_url_drops_stored_and_keeps_new(link):
    save_news_items([_item(link)], save_summary=False, skip_existing=True)
    stored_variant = _item(f"{link}?ref=y")
    new = _item(f"{link}-other")

    kept = remove_stored_by_url([stored_variant, new])

    assert kept == [new]


def test_only_triple_extracted_news_is_summarize_target(link):
    item = _item(link)
    save_news_items([item], save_summary=False, skip_existing=True)
    news_id = item["_news_id"]

    def targets():
        return [i["_news_id"] for i in fetch_unsummarized_news_items(limit=100000)]

    assert news_id not in targets()  # 미시도(NULL)
    mark_triple_extraction_result([], [news_id])
    assert news_id not in targets()  # 삼중항 없음(FALSE)
    mark_triple_extraction_result([news_id], [])
    assert news_id in targets()  # 삼중항 있음(TRUE)
