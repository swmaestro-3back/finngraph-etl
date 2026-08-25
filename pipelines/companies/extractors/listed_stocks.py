"""Neo4j 시드의 원천이 되는 법인 조회(Postgres).

그래프 시드 입장에서 companies 테이블은 **원천**이다. 적재 대상(Neo4j)과 읽기 대상
(Postgres)이 다르므로 loaders가 아니라 extractors에 둔다.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

# companies 테이블에서 상장 법인만 시드 원천으로 가져온다.
# 비상장까지 가져오면 11만여 행을 날라 로더가 96%를 버리게 되므로 SQL에서 거른다.
SELECT_COMPANIES_SQL = text(
    """
    SELECT c.id AS company_id, c.name, c.ticker, c.corp_code, c.is_listed, c.country
      FROM companies AS c
     WHERE c.is_listed
       AND BTRIM(c.name) <> ''
    """
)


def fetch_companies(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(SELECT_COMPANIES_SQL).all()
    return [
        {
            "company_id": row.company_id,
            "name": row.name,
            "ticker": row.ticker,
            "corp_code": row.corp_code,
            "is_listed": row.is_listed,
            "country": row.country,
        }
        for row in rows
    ]
