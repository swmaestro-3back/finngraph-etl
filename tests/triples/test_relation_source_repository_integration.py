"""relation_source / news_companies 적재 통합 테스트.

로컬 DB 필요: docker compose up -d db 후 0002 마이그레이션 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.triples.loaders.postgres import (
    insert_news_companies,
    insert_relation_sources,
)
from pipelines.triples.references.rdb import fetch_company_ids_by_tickers

pytestmark = pytest.mark.integration


@pytest.fixture
def news_and_company():
    marker = uuid.uuid4().hex
    ticker = marker[:6].upper()
    with session_scope() as session:
        news_id = session.execute(
            text(
                """
                INSERT INTO news (title, text, link)
                VALUES (:title, '본문', :link)
                RETURNING id;
                """
            ),
            {"title": f"triples 통합테스트 {marker}", "link": f"https://example.com/t/{marker}"},
        ).scalar_one()
        company_id = session.execute(
            text(
                """
                INSERT INTO companies (name, ticker, is_listed, country)
                VALUES (:name, :ticker, TRUE, 'KR')
                RETURNING id;
                """
            ),
            {"name": f"테스트기업{marker[:8]}", "ticker": ticker},
        ).scalar_one()

    yield int(news_id), int(company_id), ticker

    with session_scope() as session:
        # relation_source / news_companies는 FK CASCADE로 함께 삭제된다
        session.execute(text("DELETE FROM news WHERE id = :id"), {"id": news_id})
        session.execute(text("DELETE FROM companies WHERE id = :id"), {"id": company_id})


def test_insert_relation_sources_is_idempotent(news_and_company):
    news_id, _, _ = news_and_company
    rows = [
        {"neo4j_id": "5:abc:1", "reason": "근거 문장 1"},
        {"neo4j_id": "5:abc:2", "reason": "근거 문장 2"},
    ]

    assert insert_relation_sources(news_id, rows) == 2
    # 재실행(재추출) 시 UNIQUE(news_id, neo4j_id) 충돌은 무시
    assert insert_relation_sources(news_id, rows) == 0

    with session_scope() as session:
        count = session.execute(
            text("SELECT count(*) FROM relation_source WHERE news_id = :id"),
            {"id": news_id},
        ).scalar_one()
    assert count == 2


def test_insert_news_companies_is_idempotent(news_and_company):
    news_id, company_id, _ = news_and_company

    assert insert_news_companies(news_id, [company_id]) == 1
    assert insert_news_companies(news_id, [company_id]) == 0


def test_fetch_company_ids_by_tickers(news_and_company):
    _, company_id, ticker = news_and_company

    mapping = fetch_company_ids_by_tickers([ticker, "존재하지않는티커"])

    assert mapping == {ticker: company_id}
