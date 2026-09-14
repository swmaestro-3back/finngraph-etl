"""news_companies repository 통합 테스트 — 종목명 → companies.id 해석.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.news_companies import fetch_company_ids_by_stock_names

pytestmark = pytest.mark.integration


@pytest.fixture
def stocks_rows():
    tag = uuid.uuid4().hex[:8]
    name = f"해석테스트{tag}"
    with session_scope() as session:
        company_id = session.execute(
            text(
                "INSERT INTO companies (name, ticker, is_listed, country) "
                "VALUES (:n, :t, true, 'KR') RETURNING id;"
            ),
            {"n": f"해석법인{tag}", "t": f"R{tag[:5]}"},
        ).scalar_one()
        common_id = session.execute(
            text(
                "INSERT INTO stocks (name, ticker, market, company_id, standard_code) "
                "VALUES (:n, :t, 'KOSPI', :c, :code) RETURNING id;"
            ),
            {"n": name, "t": f"R{tag[:5]}", "c": company_id, "code": f"KR{tag}C"},
        ).scalar_one()
        preferred_id = session.execute(
            text(
                "INSERT INTO stocks (name, ticker, market, company_id, standard_code, "
                "preferred_stock) VALUES (:n, :t, 'KOSPI', :c, :code, true) RETURNING id;"
            ),
            {"n": f"{name}우", "t": f"P{tag[:5]}", "c": company_id, "code": f"KR{tag}P"},
        ).scalar_one()
    yield {"name": name, "company_id": int(company_id)}
    with session_scope() as session:
        session.execute(
            text("DELETE FROM stocks WHERE id = ANY(:ids)"), {"ids": [common_id, preferred_id]}
        )
        session.execute(text("DELETE FROM companies WHERE id = :c"), {"c": company_id})


def test_resolves_active_common_stock_names(stocks_rows):
    resolved = fetch_company_ids_by_stock_names(
        [stocks_rows["name"], f"{stocks_rows['name']}우", "없는종목명"]
    )

    assert resolved == {stocks_rows["name"]: stocks_rows["company_id"]}


def test_empty_input_hits_no_db():
    assert fetch_company_ids_by_stock_names([]) == {}
