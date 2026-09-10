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
