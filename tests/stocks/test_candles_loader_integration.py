"""시세 적재 통합 테스트.

검증 대상은 세 가지다.

1. 단축코드 → stock_id 변환. extractor는 코드로 말하고 테이블은 id를 키로 쓴다.
2. 원천이 둘(FDR·KIS)이라 같은 날짜를 양쪽이 채운다 — 덮어쓰되 거래대금은 지우지 않는다.
3. 재실행 멱등. 매일 도는 job이라 행이 늘면 안 된다.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.common.types import DailyCandle, PeriodCandle
from pipelines.stocks.loaders.candles import (
    fetch_latest_daily_candle_dates,
    upsert_daily_candles,
    upsert_period_candles,
)
from pipelines.stocks.loaders.symbols import sync_symbols
from pipelines.stocks.models import StockSymbol

pytestmark = pytest.mark.integration

TEST_MARKET = "PYTEST_CANDLES"
SYMBOL = "999801"
UNKNOWN_SYMBOL = "999899"


def _load_stock() -> int:
    with session_scope() as session:
        sync_symbols(
            session,
            [
                StockSymbol(
                    symbol=SYMBOL,
                    standard_code=f"KR7{SYMBOL}001",
                    name="테스트캔들",
                    market=TEST_MARKET,
                )
            ],
        )
    with session_scope() as session:
        return session.execute(
            text("SELECT id FROM stocks WHERE symbol = :symbol"), {"symbol": SYMBOL}
        ).scalar()


def _daily(trade_date: date, close: str, trade_value: int | None = None) -> DailyCandle:
    price = Decimal(close)
    return DailyCandle(
        symbol=SYMBOL,
        trade_date=trade_date,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1000,
        trade_value=trade_value,
    )


def _rows() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT c.trade_date, c.close, c.trade_value, c.source
                  FROM stock_daily_candles AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.symbol = :symbol
                 ORDER BY c.trade_date
                """
            ),
            {"symbol": SYMBOL},
        )
        return [dict(row._mapping) for row in rows]


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            # 시세는 stocks를 ON DELETE CASCADE로 참조하므로 종목만 지우면 함께 사라진다.
            session.execute(
                text("DELETE FROM stocks WHERE market = :market"), {"market": TEST_MARKET}
            )

    purge()
    yield
    purge()


def test_daily_candles_are_keyed_by_stock_id() -> None:
    """단축코드가 아니라 stock_id로 저장된다."""
    stock_id = _load_stock()

    with session_scope() as session:
        inserted = upsert_daily_candles(session, [_daily(date(2026, 8, 7), "100")], source="FDR")

    assert inserted == 1
    with session_scope() as session:
        # 같은 날짜에 다른 종목의 봉이 있을 수 있으므로 이 종목으로 좁혀 확인한다.
        stored = session.execute(
            text(
                """
                SELECT c.stock_id
                  FROM stock_daily_candles AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.symbol = :symbol AND c.trade_date = :d
                """
            ),
            {"symbol": SYMBOL, "d": date(2026, 8, 7)},
        ).scalar()
    assert stored == stock_id


def test_unknown_symbol_is_skipped() -> None:
    """적재되지 않은 종목의 봉은 조용히 버린다 — 외래키 오류로 배치를 죽이지 않는다."""
    _load_stock()
    unknown = DailyCandle(
        symbol=UNKNOWN_SYMBOL,
        trade_date=date(2026, 8, 7),
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        volume=1,
    )

    with session_scope() as session:
        inserted = upsert_daily_candles(session, [unknown, _daily(date(2026, 8, 7), "100")])

    assert inserted == 1


def test_kis_overwrites_fdr_but_keeps_trade_value() -> None:
    """원천이 둘이라 덮어쓰되, 거래대금이 없는 원천이 기존 값을 지우면 안 된다."""
    _load_stock()

    with session_scope() as session:
        upsert_daily_candles(
            session, [_daily(date(2026, 8, 7), "100", trade_value=5000)], source="KIS"
        )
    with session_scope() as session:
        # FDR은 거래대금을 주지 않아 None으로 들어온다.
        upsert_daily_candles(session, [_daily(date(2026, 8, 7), "110")], source="FDR")

    rows = _rows()
    assert len(rows) == 1
    assert rows[0]["close"] == Decimal("110")
    assert rows[0]["trade_value"] == 5000
    assert rows[0]["source"] == "FDR"


def test_rerun_is_idempotent() -> None:
    """매일 도는 job이라 두 번째 실행에서 행이 늘면 안 된다."""
    _load_stock()
    candles = [_daily(date(2026, 8, 6), "100"), _daily(date(2026, 8, 7), "110")]

    with session_scope() as session:
        upsert_daily_candles(session, candles)
    with session_scope() as session:
        upsert_daily_candles(session, candles)

    assert len(_rows()) == 2


def test_latest_daily_candle_dates_drive_backfill_start() -> None:
    """백필은 이미 적재된 마지막 거래일 다음날부터 받는다."""
    stock_id = _load_stock()

    with session_scope() as session:
        upsert_daily_candles(
            session, [_daily(date(2026, 8, 6), "100"), _daily(date(2026, 8, 7), "110")]
        )

    with session_scope() as session:
        latest = fetch_latest_daily_candle_dates(session)

    assert latest[stock_id] == date(2026, 8, 7)


def test_period_candles_separate_week_and_month() -> None:
    """같은 날짜라도 주봉과 월봉은 다른 행이다."""
    _load_stock()
    price = Decimal("100")
    candles = [
        PeriodCandle(
            symbol=SYMBOL,
            period=period,
            base_date=date(2026, 8, 7),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=10,
        )
        for period in ("W", "M")
    ]

    with session_scope() as session:
        upsert_period_candles(session, candles)
    with session_scope() as session:
        upsert_period_candles(session, candles)

    with session_scope() as session:
        count = session.execute(
            text(
                """
                SELECT count(*) FROM stock_period_candles AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.symbol = :symbol
                """
            ),
            {"symbol": SYMBOL},
        ).scalar()

    assert count == 2
