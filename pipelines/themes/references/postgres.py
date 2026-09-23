"""
postgresql 참조용 모듈
"""

from __future__ import annotations

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 특정 종목 이름 및 티커 조회
SELECT_STOCK_NAMES_SQL = text(
    """
    SELECT ticker, name
      FROM stocks
     WHERE ticker = ANY(:tickers)
       AND is_active;
    """
)

# 테마별 편입 종목 이름 및 티커 조회
SELECT_EXISTING_THEMES_SQL = text(
    """
    SELECT t.name, s.ticker
      FROM themes t
      LEFT JOIN theme_stocks ts ON ts.theme_id = t.id
      LEFT JOIN stocks s ON s.id = ts.stock_id;
    """
)

# 테마 PK 조회. Neo4j Theme.theme_id 의 원천이다.
SELECT_THEME_IDS_SQL = text(
    """
    SELECT id, name
      FROM themes;
    """
)


def fetch_stock_names_by_tickers(tickers: list[str]) -> dict[str, str]:

    unique_tickers = sorted({ticker for ticker in tickers if ticker})

    if not unique_tickers:
        return {}

    with session_scope() as session:
        rows = session.execute(SELECT_STOCK_NAMES_SQL, {"tickers": unique_tickers}).fetchall()

    return {ticker: name for ticker, name in rows}


def fetch_existing_themes() -> dict[str, set[str]]:

    existing: dict[str, set[str]] = {}

    with session_scope() as session:
        rows = session.execute(SELECT_EXISTING_THEMES_SQL).fetchall()

    for name, ticker in rows:
        stocks = existing.setdefault(name, set())
        if ticker:
            stocks.add(ticker)

    return existing


def fetch_theme_ids() -> dict[str, int]:
    """전체 테마의 {name: id}. 테마 수가 적어 매 회차 전량 조회해도 부담이 없다."""

    with session_scope() as session:
        rows = session.execute(SELECT_THEME_IDS_SQL).fetchall()

    return {name: theme_id for theme_id, name in rows}
