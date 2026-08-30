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
#
# market은 companies에 없어 stocks에서 얻는다. 활성 종목의 단축코드는 부분 유니크
# (stocks_active_ticker_uk)라 조인이 행을 불리지 않는다. LEFT JOIN인 이유: INNER면
# 짝이 없는 상장 법인이 시드에서 빠지고, 로더의 폐지 삭제가 그 노드를 지워버린다.
SELECT_COMPANIES_SQL = text(
    """
    SELECT c.id AS company_id, c.name, c.ticker, c.corp_code, c.is_listed, c.country,
           s.market
      FROM companies AS c
      LEFT JOIN stocks AS s
        ON s.ticker = c.ticker
       AND s.is_active
     WHERE c.is_listed
       AND c.ticker IS NOT NULL
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
            "market": row.market,
        }
        for row in rows
    ]
