"""fetch_trade_dates 통합 테스트 — 마감일(settled)·최신일(latest) 범위.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid
from datetime import date

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.stocks.repositories.postgres.stock_candles import fetch_trade_dates
from pipelines.stocks.types import TradeDates

pytestmark = pytest.mark.integration

# 실제 데이터와 겹치지 않도록 먼 미래 날짜를 쓴다
SETTLED = date(2099, 1, 7)
INTRADAY = date(2099, 1, 8)


@pytest.fixture
def settled_stock():
    """SETTLED 에 일봉·밸류에이션이 둘 다 있는 종목 1개."""

    tag = uuid.uuid4().hex[:8]
    with session_scope() as session:
        stock_id = session.execute(
            text(
                "INSERT INTO stocks (name, ticker, market, standard_code) "
                "VALUES (:n, :t, 'KOSDAQ', :code) RETURNING id;"
            ),
            {"n": f"거래일종목{tag}", "t": f"D{tag[:5]}", "code": f"KR{tag}D"},
        ).scalar_one()
        session.execute(
            text(
                "INSERT INTO stock_candles_daily "
                "(stock_id, trade_date, open, high, low, close, volume, source) "
                "VALUES (:s, :d, 100, 100, 100, 100, 1, 'TEST');"
            ),
            {"s": stock_id, "d": SETTLED},
        )
        session.execute(
            text(
                "INSERT INTO stock_valuations_daily (listing_id, trade_date, market_cap) "
                "VALUES (:s, :d, 1);"
            ),
            {"s": stock_id, "d": SETTLED},
        )

    yield stock_id

    with session_scope() as session:
        # stocks 삭제가 stock_candles_daily 를 CASCADE 로 지운다
        session.execute(
            text("DELETE FROM stock_valuations_daily WHERE listing_id = :s"), {"s": stock_id}
        )
        session.execute(text("DELETE FROM stocks WHERE id = :s"), {"s": stock_id})


def test_trade_dates_converge_once_valuations_are_loaded(settled_stock):
    with session_scope() as session:
        dates = fetch_trade_dates(session, as_of=SETTLED)

    assert dates == TradeDates(settled=SETTLED, latest=SETTLED)


def test_intraday_candles_advance_latest_but_not_settled(settled_stock):
    with session_scope() as session:
        session.execute(
            text(
                "INSERT INTO stock_candles_daily "
                "(stock_id, trade_date, open, high, low, close, volume, source) "
                "VALUES (:s, :d, 120, 120, 120, 120, 1, 'TEST');"
            ),
            {"s": settled_stock, "d": INTRADAY},
        )

    with session_scope() as session:
        dates = fetch_trade_dates(session, as_of=INTRADAY)

    assert dates == TradeDates(settled=SETTLED, latest=INTRADAY)
