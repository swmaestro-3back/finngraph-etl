"""RDB 참조 데이터 조회.

news_companies 행을 만들 때 ticker를 companies.id로 해석해야 한다.
transformer가 쓰는 읽기 전용 참조라 references/에 둔다 (쓰기는 loaders/postgres.py).
"""

from __future__ import annotations

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope


def fetch_company_ids_by_tickers(tickers: list[str]) -> dict[str, int]:
    """ticker 목록을 활성(비상폐) companies.id로 매핑한다. 미상장/상폐는 결과에 없다."""

    unique_tickers = sorted({ticker for ticker in tickers if ticker})

    if not unique_tickers:
        return {}

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT ticker, id
                FROM companies
                WHERE ticker = ANY(:tickers)
                  AND delisted_at IS NULL;
                """
            ),
            {"tickers": unique_tickers},
        ).fetchall()

    return {ticker: int(company_id) for ticker, company_id in rows}
