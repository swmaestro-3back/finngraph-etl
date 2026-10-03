"""테마 봉 계산용 구성 종목 조회.

구성 종목 조회는 theme_stocks ⋈ stocks ⋈ stock_candles_daily 를 그대로 돌려주고, 참여 여부
판정(상장주식수, 전일 봉 유무)은 transformer 가 한다 — 규칙이 한 곳에만 있게 하려는 것이다.

theme_stocks 적재는 themes.py 의 load_themes 가 테마 행과 함께 한다.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.themes.types import ConstituentCandle

FETCH_THEME_IDS_SQL = text("SELECT DISTINCT theme_id FROM theme_stocks ORDER BY theme_id")

FETCH_CONSTITUENT_CANDLES_SQL = text(
    """
    SELECT c.stock_id, c.trade_date, c.open, c.high, c.low, c.close,
           c.volume, c.trade_value, s.listed_shares
      FROM theme_stocks AS ts
      JOIN stocks AS s ON s.id = ts.stock_id
      JOIN stock_candles_daily AS c ON c.stock_id = ts.stock_id
     WHERE ts.theme_id = :theme_id
       AND c.trade_date BETWEEN :since AND :end
     ORDER BY c.trade_date, c.stock_id
    """
)


def fetch_theme_ids(session: Session) -> list[int]:
    """편입 종목이 하나라도 있는 테마 id."""

    return [row[0] for row in session.execute(FETCH_THEME_IDS_SQL)]


def fetch_constituent_candles(
    session: Session, theme_id: int, since: date, end: date
) -> list[ConstituentCandle]:
    """테마 구성 종목의 [since, end] 일봉. since 는 보통 캘린더 첫 거래일(t-1 로만 쓰는 날)이다."""

    rows = session.execute(
        FETCH_CONSTITUENT_CANDLES_SQL, {"theme_id": theme_id, "since": since, "end": end}
    )
    return [
        ConstituentCandle(
            stock_id=row.stock_id,
            trade_date=row.trade_date,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            trade_value=row.trade_value,
            listed_shares=row.listed_shares,
        )
        for row in rows
    ]
