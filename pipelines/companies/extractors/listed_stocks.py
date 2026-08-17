"""Neo4j 시드의 원천이 되는 상장 보통주 조회(Postgres).

그래프 시드 입장에서 stocks 테이블은 **원천**이다. 적재 대상(Neo4j)과 읽기 대상
(Postgres)이 다르므로 loaders가 아니라 extractors에 둔다.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

# 시드 원천: 활성 보통주만. companies upsert와 같은 필터(수집 범위 ≠ 제공 범위).
SELECT_LISTED_COMMON_STOCKS_SQL = text(
    """
    SELECT s.name, s.ticker, s.market
      FROM stocks AS s
     WHERE s.is_active
       AND NOT s.preferred_stock
       AND NOT s.etp
       AND NOT s.spac
       AND BTRIM(s.name) <> ''
    """
)


def fetch_listed_common_stocks(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(SELECT_LISTED_COMMON_STOCKS_SQL).all()
    return [{"name": row.name, "ticker": row.ticker, "market": row.market} for row in rows]
