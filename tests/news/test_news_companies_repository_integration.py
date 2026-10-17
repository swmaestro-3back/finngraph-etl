"""news_companies INSERT 통합 테스트.

로컬 DB 필요: docker compose up -d db 후 마이그레이션 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_companies import insert_news_companies

pytestmark = pytest.mark.integration


@pytest.fixture
def news_and_companies():
    marker = uuid.uuid4().hex
    with session_scope() as session:
        news_id = session.execute(
            text(
                "INSERT INTO news (title, text, link) VALUES (:title, '본문', :link) RETURNING id"
            ),
            {
                "title": f"news_companies 통합테스트 {marker}",
                "link": f"https://example.com/nc/{marker}",
            },
        ).scalar_one()
        company_ids = [
            session.execute(
                text(
                    """
                    INSERT INTO companies (name, ticker, is_listed, country)
                    VALUES (:name, :ticker, TRUE, 'KR')
                    RETURNING id
                    """
                ),
                {
                    "name": f"테스트기업{marker[:8]}{suffix}",
                    "ticker": f"{marker[:5]}{suffix}".upper(),
                },
            ).scalar_one()
            for suffix in ("A", "B")
        ]

    yield int(news_id), [int(company_id) for company_id in company_ids]

    with session_scope() as session:
        # news_companies 는 FK CASCADE 로 함께 삭제된다
        session.execute(text("DELETE FROM news WHERE id = :id"), {"id": news_id})
        session.execute(text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": company_ids})


def test_insert_news_companies_is_idempotent_and_dedupes(news_and_companies):
    news_id, (first, second) = news_and_companies

    assert insert_news_companies(news_id, [first, second, first]) == 2
    assert insert_news_companies(news_id, [first, second]) == 0
    assert insert_news_companies(news_id, []) == 0
