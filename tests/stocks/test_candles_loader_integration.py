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

from pipelines.common.clients.postgres import session_scope
from pipelines.stocks.jobs import calculate_change_rates
from pipelines.stocks.models import StockTicker
from pipelines.stocks.repositories.postgres.stock_candles import (
    aggregate_current_period_candles,
    refresh_daily_change_rates,
    refresh_period_change_rates,
    upsert_daily_candles,
    upsert_period_candles,
)
from pipelines.stocks.repositories.postgres.stocks import sync_tickers
from pipelines.stocks.types import DailyCandle, PeriodCandle

pytestmark = pytest.mark.integration

TEST_MARKET = "PYTEST_CANDLES"
TICKER = "999801"
UNKNOWN_TICKER = "999899"


def _load_stock() -> int:
    with session_scope() as session:
        sync_tickers(
            session,
            [
                StockTicker(
                    ticker=TICKER,
                    standard_code=f"KR7{TICKER}001",
                    name="테스트캔들",
                    market=TEST_MARKET,
                )
            ],
        )
    with session_scope() as session:
        return session.execute(
            text("SELECT id FROM stocks WHERE ticker = :ticker"), {"ticker": TICKER}
        ).scalar()


def _daily(
    trade_date: date,
    close: str,
    trade_value: int | None = None,
    base_price: str | None = None,
) -> DailyCandle:
    price = Decimal(close)
    return DailyCandle(
        ticker=TICKER,
        trade_date=trade_date,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1000,
        trade_value=trade_value,
        base_price=Decimal(base_price) if base_price is not None else None,
    )


def _base_prices() -> list[tuple]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT c.trade_date, c.base_price FROM stock_candles_daily AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker
                 ORDER BY c.trade_date
                """
            ),
            {"ticker": TICKER},
        )
        return [tuple(row) for row in rows]


def _rows() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT c.trade_date, c.close, c.trade_value, c.source
                  FROM stock_candles_daily AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker
                 ORDER BY c.trade_date
                """
            ),
            {"ticker": TICKER},
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
                  FROM stock_candles_daily AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker AND c.trade_date = :d
                """
            ),
            {"ticker": TICKER, "d": date(2026, 8, 7)},
        ).scalar()
    assert stored == stock_id


def test_unknown_ticker_is_skipped() -> None:
    """적재되지 않은 종목의 봉은 조용히 버린다 — 외래키 오류로 배치를 죽이지 않는다."""
    _load_stock()
    unknown = DailyCandle(
        ticker=UNKNOWN_TICKER,
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


def test_period_candles_separate_week_and_month() -> None:
    """같은 날짜라도 주봉과 월봉은 다른 행이다."""
    _load_stock()
    price = Decimal("100")
    candles = [
        PeriodCandle(
            ticker=TICKER,
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
                SELECT count(*) FROM stock_candles_period AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker
                """
            ),
            {"ticker": TICKER},
        ).scalar()

    assert count == 2


def _period(period: str, base_date: date, close: str) -> PeriodCandle:
    price = Decimal(close)
    return PeriodCandle(
        ticker=TICKER,
        period=period,
        base_date=base_date,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=10,
    )


def _period_rows(period: str) -> list[tuple[date, Decimal]]:
    with session_scope() as session:
        return session.execute(
            text(
                """
                SELECT c.base_date, c.close FROM stock_candles_period AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker AND c.period = :period
                 ORDER BY c.base_date
                """
            ),
            {"ticker": TICKER, "period": period},
        ).all()


def test_monthly_candle_updates_same_row_within_month() -> None:
    """진행 중인 달은 KIS가 조회일을 base_date로 주지만 1일로 정규화되어 한 행만 갱신된다."""
    _load_stock()

    for day, close in ((11, "100"), (15, "110"), (29, "120")):
        with session_scope() as session:
            upsert_period_candles(session, [_period("M", date(2026, 9, day), close)])

    assert _period_rows("M") == [(date(2026, 9, 1), Decimal("120"))]


def test_weekly_candle_is_keyed_by_monday() -> None:
    """휴장으로 화요일에 시작한 주도 월요일 키로 들어간다."""
    _load_stock()

    with session_scope() as session:
        upsert_period_candles(session, [_period("W", date(2026, 8, 18), "100")])

    assert _period_rows("W") == [(date(2026, 8, 17), Decimal("100"))]


def test_aggregate_current_period_from_daily_candles() -> None:
    """최신 일봉이 속한 주·월 봉만 일봉으로 합성한다. 지난 구간 행은 건드리지 않는다."""
    _load_stock()
    daily = [
        DailyCandle(
            ticker=TICKER,
            trade_date=date(2026, 9, 25),
            open=Decimal("90"),
            high=Decimal("95"),
            low=Decimal("85"),
            close=Decimal("92"),
            volume=5,
            trade_value=50,
        ),
        DailyCandle(
            ticker=TICKER,
            trade_date=date(2026, 9, 28),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("99"),
            close=Decimal("105"),
            volume=10,
            trade_value=100,
        ),
        DailyCandle(
            ticker=TICKER,
            trade_date=date(2026, 9, 29),
            open=Decimal("106"),
            high=Decimal("120"),
            low=Decimal("101"),
            close=Decimal("115"),
            volume=20,
            trade_value=200,
        ),
    ]
    with session_scope() as session:
        upsert_daily_candles(session, daily)
        upsert_period_candles(
            session, [_period("W", date(2026, 9, 21), "1"), _period("M", date(2026, 8, 1), "1")]
        )
        rows = aggregate_current_period_candles(session, date(2026, 9, 1))

    # 종목을 좁힐 수 없는 집계라 로컬 DB 의 다른 종목도 함께 센다. 이 종목 몫(주·월 2행)만
    # 하한으로 보고, 정확한 행은 아래에서 이 종목으로 좁혀 확인한다.
    assert rows >= 2
    with session_scope() as session:
        got = session.execute(
            text(
                """
                SELECT c.period, c.base_date, c.open, c.high, c.low, c.close, c.volume, c.trade_value
                  FROM stock_candles_period AS c JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker ORDER BY c.period, c.base_date
                """
            ),
            {"ticker": TICKER},
        ).all()
    assert [tuple(r) for r in got] == [
        ("M", date(2026, 8, 1), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), 10, None),
        (
            "M",
            date(2026, 9, 1),
            Decimal("90"),
            Decimal("120"),
            Decimal("85"),
            Decimal("115"),
            35,
            350,
        ),
        ("W", date(2026, 9, 21), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), 10, None),
        (
            "W",
            date(2026, 9, 28),
            Decimal("100"),
            Decimal("120"),
            Decimal("99"),
            Decimal("115"),
            30,
            300,
        ),
    ]


def _daily_change_rates() -> list[tuple]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT c.trade_date, c.change_rate FROM stock_candles_daily AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker
                 ORDER BY c.trade_date
                """
            ),
            {"ticker": TICKER},
        )
        return [tuple(row) for row in rows]


def _period_change_rates(period: str) -> list[tuple]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT c.base_date, c.change_rate FROM stock_candles_period AS c
                  JOIN stocks AS s ON s.id = c.stock_id
                 WHERE s.ticker = :ticker AND c.period = :period
                 ORDER BY c.base_date
                """
            ),
            {"ticker": TICKER, "period": period},
        )
        return [tuple(row) for row in rows]


def test_daily_change_rate_is_vs_previous_bar() -> None:
    """직전 봉 종가 대비 %. 첫 봉은 NULL 이고, 거래가 없던 날은 건너뛰어 마지막 봉과 비교한다."""
    _load_stock()

    with session_scope() as session:
        upsert_daily_candles(
            session,
            [
                _daily(date(2026, 8, 5), "100"),
                _daily(date(2026, 8, 6), "110"),
                # 8/7 은 봉이 없다(거래정지).
                _daily(date(2026, 8, 10), "99"),
            ],
        )
        refresh_daily_change_rates(session, since=date(2026, 8, 5))

    assert _daily_change_rates() == [
        (date(2026, 8, 5), None),
        (date(2026, 8, 6), Decimal("10.00")),
        (date(2026, 8, 10), Decimal("-10.00")),
    ]


def test_daily_change_rate_uses_bar_before_since_and_refreshes_later_bars() -> None:
    """since 첫 행은 그 앞의 봉과 비교하고, 과거 종가가 바뀌면 그 뒤 봉도 다시 계산한다."""
    stock_id = _load_stock()
    with session_scope() as session:
        upsert_daily_candles(
            session, [_daily(date(2026, 8, 5), "100"), _daily(date(2026, 8, 7), "120")]
        )
        refresh_daily_change_rates(session, since=date(2026, 8, 5), stock_ids=[stock_id])

    with session_scope() as session:
        # 가운데 날만 적재한다. 8/6 은 8/5 대비, 8/7 은 새로 생긴 8/6 대비가 된다.
        upsert_daily_candles(session, [_daily(date(2026, 8, 6), "80")])
        changed = refresh_daily_change_rates(session, since=date(2026, 8, 6), stock_ids=[stock_id])

    assert changed == 2

    assert _daily_change_rates() == [
        (date(2026, 8, 5), None),
        (date(2026, 8, 6), Decimal("-20.00")),
        (date(2026, 8, 7), Decimal("50.00")),
    ]


def test_base_price_is_stored_and_overwritten_by_latest_fetch() -> None:
    _load_stock()

    with session_scope() as session:
        upsert_daily_candles(session, [_daily(date(2026, 10, 1), "212000", base_price="212500")])
    assert _base_prices() == [(date(2026, 10, 1), Decimal("212500"))]

    with session_scope() as session:
        upsert_daily_candles(session, [_daily(date(2026, 10, 1), "42400", base_price="42500")])
    assert _base_prices() == [(date(2026, 10, 1), Decimal("42500"))]

    with session_scope() as session:
        upsert_daily_candles(session, [_daily(date(2026, 10, 1), "42400")])
    assert _base_prices() == [(date(2026, 10, 1), None)]


def test_daily_change_rate_prefers_base_price_over_previous_close() -> None:
    stock_id = _load_stock()

    with session_scope() as session:
        upsert_daily_candles(
            session,
            [
                _daily(date(2026, 9, 29), "100", base_price="98"),
                _daily(date(2026, 9, 30), "110", base_price="105"),
                _daily(date(2026, 10, 1), "99"),
                _daily(date(2026, 10, 2), "99", base_price="99"),
            ],
        )
        refresh_daily_change_rates(session, since=date(2026, 9, 29), stock_ids=[stock_id])

    assert _daily_change_rates() == [
        (date(2026, 9, 29), Decimal("2.04")),
        (date(2026, 9, 30), Decimal("4.76")),
        (date(2026, 10, 1), Decimal("-10.00")),
        (date(2026, 10, 2), Decimal("0.00")),
    ]


def test_daily_change_rate_follows_overwritten_base_price() -> None:
    stock_id = _load_stock()
    with session_scope() as session:
        upsert_daily_candles(
            session,
            [_daily(date(2026, 9, 30), "100"), _daily(date(2026, 10, 1), "110", base_price="104")],
        )
        refresh_daily_change_rates(session, since=date(2026, 9, 30), stock_ids=[stock_id])

    with session_scope() as session:
        upsert_daily_candles(session, [_daily(date(2026, 10, 1), "110")])
        changed = refresh_daily_change_rates(session, since=date(2026, 9, 30), stock_ids=[stock_id])

    assert changed == 1
    assert _daily_change_rates() == [
        (date(2026, 9, 30), None),
        (date(2026, 10, 1), Decimal("10.00")),
    ]


def test_period_change_rate_is_vs_previous_period_bar() -> None:
    """주봉은 직전 주봉, 월봉은 직전 월봉 대비다. 서로 섞이지 않는다."""
    _load_stock()

    with session_scope() as session:
        upsert_period_candles(
            session,
            [
                _period("W", date(2026, 8, 10), "100"),
                _period("W", date(2026, 8, 17), "105"),
                _period("M", date(2026, 7, 1), "200"),
                _period("M", date(2026, 8, 1), "150"),
            ],
        )
        refresh_period_change_rates(session, since=date(2026, 7, 1))

    assert _period_change_rates("W") == [
        (date(2026, 8, 10), None),
        (date(2026, 8, 17), Decimal("5.00")),
    ]
    assert _period_change_rates("M") == [
        (date(2026, 7, 1), None),
        (date(2026, 8, 1), Decimal("-25.00")),
    ]


def test_aggregated_period_candle_gets_change_rate() -> None:
    """장중에 일봉으로 합성한 이번 주·이번 달 봉에도 등락률이 채워진다."""
    _load_stock()
    with session_scope() as session:
        upsert_period_candles(
            session, [_period("W", date(2026, 9, 21), "100"), _period("M", date(2026, 8, 1), "92")]
        )
        upsert_daily_candles(session, [_daily(date(2026, 9, 28), "115")])
        aggregate_current_period_candles(session, date(2026, 9, 28))
        # 이번 주 중간 날짜를 줘도 그 주·그 달의 봉부터 다시 계산한다.
        refresh_period_change_rates(session, since=date(2026, 9, 30))

    assert _period_change_rates("W") == [
        (date(2026, 9, 21), None),
        (date(2026, 9, 28), Decimal("15.00")),
    ]
    assert _period_change_rates("M") == [
        (date(2026, 8, 1), None),
        (date(2026, 9, 1), Decimal("25.00")),
    ]


def test_change_rate_jobs_narrow_by_ticker() -> None:
    """job 은 단축코드로 대상을 좁히고, 없는 단축코드만 주면 아무것도 건드리지 않는다."""
    _load_stock()
    with session_scope() as session:
        upsert_daily_candles(
            session, [_daily(date(2026, 8, 5), "100"), _daily(date(2026, 8, 6), "110")]
        )
        upsert_period_candles(
            session,
            [_period("W", date(2026, 8, 10), "100"), _period("W", date(2026, 8, 17), "105")],
        )

    assert calculate_change_rates.run_daily(since=date(2026, 8, 5), tickers=[UNKNOWN_TICKER]) == 0
    assert calculate_change_rates.run_period(since=date(2026, 8, 10), tickers=[UNKNOWN_TICKER]) == 0
    assert calculate_change_rates.run_daily(since=date(2026, 8, 5), tickers=[TICKER]) == 1
    assert calculate_change_rates.run_period(since=date(2026, 8, 10), tickers=[TICKER]) == 1

    assert _daily_change_rates() == [(date(2026, 8, 5), None), (date(2026, 8, 6), Decimal("10.00"))]
    assert _period_change_rates("W") == [
        (date(2026, 8, 10), None),
        (date(2026, 8, 17), Decimal("5.00")),
    ]
