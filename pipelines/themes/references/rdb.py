"""RDB 참조 데이터 조회.

`references/`는 이 파이프라인이 **읽기만 하는** 참조 계층이다. stocks 는 stocks
도메인(sync_tickers)이 채우고 themes 는 절대 쓰지 않는다.
"""

from __future__ import annotations

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 활성 종목의 ticker 는 부분 유니크(stocks_active_ticker_uk)라 중복이 없다.
SELECT_STOCK_NAMES_SQL = text(
    """
    SELECT ticker, name
      FROM stocks
     WHERE ticker = ANY(:tickers)
       AND is_active;
    """
)


def fetch_stock_names_by_tickers(tickers: list[str]) -> dict[str, str]:
    """ticker → 종목명. 크롤링한 기업명이 옳은지 대조하는 데 쓴다.

    ticker를 키로 잡는 이유: 사명은 바뀌어도 ticker는 유지되므로 저장소와 크롤링
    결과를 잇는 안정적인 식별자다.
    """

    unique_tickers = sorted({ticker for ticker in tickers if ticker})

    if not unique_tickers:
        return {}

    with session_scope() as session:
        rows = session.execute(SELECT_STOCK_NAMES_SQL, {"tickers": unique_tickers}).fetchall()

    return {ticker: name for ticker, name in rows}
