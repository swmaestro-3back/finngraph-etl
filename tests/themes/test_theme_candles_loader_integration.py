"""테마 봉 로더·잡 통합 테스트 (실제 Postgres).

검증 대상:
1. 캘린더는 [start, end] 의 거래일만 돌려준다(첫 원소는 잡이 t-1 로만 쓴다).
2. 구성 종목 조회는 theme_stocks ⋈ stocks ⋈ stock_candles_daily 를 그대로 돌려준다.
3. 일봉 upsert 는 멱등이다.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.stocks.loaders.candles import upsert_daily_candles
from pipelines.stocks.loaders.tickers import sync_tickers
from pipelines.stocks.models import StockTicker
from pipelines.stocks.types import DailyCandle
from pipelines.themes.jobs.calculate_theme_candles import run_change_rates, run_daily, run_period
from pipelines.themes.loaders.candles import (
    fetch_anchors,
    fetch_constituent_candles,
    fetch_theme_ids,
    fetch_trading_calendar,
    rebuild_theme_period_candles,
    refresh_theme_change_rates,
    upsert_theme_daily_candles,
)
from pipelines.themes.types import ThemeCandle

pytestmark = pytest.mark.integration

TEST_MARKET = "PYTEST_THEME_CANDLES"
THEME_NAME = "PYTEST_테마봉"
THEME_NAME_2 = "PYTEST_테마봉2"
TICKERS = ("999811", "999812")


def _setup(listed_shares: dict[str, int | None] | None = None) -> tuple[int, dict[str, int]]:
    """테마 하나와 종목 두 개를 만들고 (theme_id, ticker → stock_id) 를 돌려준다."""

    shares = listed_shares or {t: 1000 for t in TICKERS}
    with session_scope() as session:
        sync_tickers(
            session,
            [
                StockTicker(
                    ticker=t,
                    standard_code=f"KR7{t}001",
                    name=f"테스트{t}",
                    market=TEST_MARKET,
                    listed_shares=shares[t],
                )
                for t in TICKERS
            ],
        )
    with session_scope() as session:
        stock_ids = dict(
            session.execute(
                text("SELECT ticker, id FROM stocks WHERE market = :m"), {"m": TEST_MARKET}
            ).all()
        )
        theme_id = session.execute(
            text("INSERT INTO themes (name) VALUES (:n) RETURNING id"), {"n": THEME_NAME}
        ).scalar()
        session.execute(
            text("INSERT INTO theme_stocks (theme_id, stock_id) VALUES (:t, :s)"),
            [{"t": theme_id, "s": sid} for sid in stock_ids.values()],
        )
    return theme_id, stock_ids


def _daily(ticker: str, day: date, close: str, trade_value: int | None = 100) -> DailyCandle:
    c = Decimal(close)
    return DailyCandle(
        ticker=ticker,
        trade_date=day,
        open=c,
        high=c,
        low=c,
        close=c,
        volume=10,
        trade_value=trade_value,
    )


def _load_daily(candles: list[DailyCandle]) -> None:
    with session_scope() as session:
        upsert_daily_candles(session, candles, source="KIS")


def _theme_rows(theme_id: int) -> list[tuple[date, Decimal]]:
    with session_scope() as session:
        return session.execute(
            text(
                "SELECT trade_date, close FROM theme_candles_daily"
                " WHERE theme_id = :t ORDER BY trade_date"
            ),
            {"t": theme_id},
        ).all()


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            # 종목을 지우면 일봉·theme_stocks 가, 테마를 지우면 테마 봉이 CASCADE 로 사라진다.
            session.execute(text("DELETE FROM stocks WHERE market = :m"), {"m": TEST_MARKET})
            session.execute(
                text("DELETE FROM themes WHERE name = ANY(:n)"), {"n": [THEME_NAME, THEME_NAME_2]}
            )

    purge()
    yield
    purge()


D1, D2, D3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)


def test_trading_calendar_is_window_only() -> None:
    _setup()
    _load_daily([_daily(TICKERS[0], d, "100") for d in (D1, D2, D3)])

    with session_scope() as session:
        cal = fetch_trading_calendar(session, start=D2, end=D3)

    assert cal == [D2, D3]
    assert D1 not in cal


def test_theme_ids_only_include_themes_with_stocks() -> None:
    theme_id, _ = _setup()

    with session_scope() as session:
        ids = fetch_theme_ids(session)

    assert theme_id in ids


def test_constituent_candles_join_listed_shares() -> None:
    theme_id, stock_ids = _setup({TICKERS[0]: 1000, TICKERS[1]: None})
    _load_daily([_daily(TICKERS[0], D1, "100"), _daily(TICKERS[1], D1, "200")])

    with session_scope() as session:
        rows = fetch_constituent_candles(session, theme_id, since=D1, end=D1)

    by_stock = {r.stock_id: r for r in rows}
    assert by_stock[stock_ids[TICKERS[0]]].listed_shares == 1000
    assert by_stock[stock_ids[TICKERS[1]]].listed_shares is None
    assert by_stock[stock_ids[TICKERS[0]]].close == Decimal("100")


def test_upsert_theme_daily_is_idempotent_and_returns_anchor() -> None:
    theme_id, _ = _setup()
    candle = ThemeCandle(
        theme_id=theme_id,
        trade_date=D1,
        open=Decimal("1000"),
        high=Decimal("1000"),
        low=Decimal("1000"),
        close=Decimal("1010.5"),
        volume=1,
    )

    with session_scope() as session:
        assert upsert_theme_daily_candles(session, [candle]) == 1
    with session_scope() as session:
        assert upsert_theme_daily_candles(session, [candle]) == 1
        anchors = fetch_anchors(session, [theme_id], before=D2)
        none_before = fetch_anchors(session, [theme_id], before=D1)

    assert _theme_rows(theme_id) == [(D1, Decimal("1010.5"))]
    assert anchors == {theme_id: Decimal("1010.5")}
    assert none_before == {}


def _period_rows(theme_id: int, period: str) -> list[tuple]:
    with session_scope() as session:
        return session.execute(
            text(
                "SELECT base_date, open, high, low, close, volume, trade_value"
                "  FROM theme_candles_period WHERE theme_id = :t AND period = :p"
                " ORDER BY base_date"
            ),
            {"t": theme_id, "p": period},
        ).all()


def _theme_candle(theme_id: int, day: date, o: str, h: str, lo: str, c: str) -> ThemeCandle:
    return ThemeCandle(
        theme_id=theme_id,
        trade_date=day,
        open=Decimal(o),
        high=Decimal(h),
        low=Decimal(lo),
        close=Decimal(c),
        volume=10,
        trade_value=100,
    )


def test_rebuild_period_candles_across_week_and_month_boundary() -> None:
    """9/25(금, 9월 4주) · 9/28·29(월·화, 9월 5주). since=9/28 이면 이번 주·9월 전체가 대상."""
    theme_id, _ = _setup()
    with session_scope() as session:
        upsert_theme_daily_candles(
            session,
            [
                _theme_candle(theme_id, date(2026, 9, 25), "90", "95", "85", "92"),
                _theme_candle(theme_id, date(2026, 9, 28), "100", "110", "99", "105"),
                _theme_candle(theme_id, date(2026, 9, 29), "106", "120", "101", "115"),
            ],
        )

    with session_scope() as session:
        rows = rebuild_theme_period_candles(session, since=date(2026, 9, 28))

    # W: 9/28 주 1행. M: 9월 1행 (since 를 월초로 내리므로 9/25 도 포함). 합계 2행.
    assert rows == 2
    assert _period_rows(theme_id, "W") == [
        (date(2026, 9, 28), Decimal("100"), Decimal("120"), Decimal("99"), Decimal("115"), 20, 200)
    ]
    assert _period_rows(theme_id, "M") == [
        (date(2026, 9, 1), Decimal("90"), Decimal("120"), Decimal("85"), Decimal("115"), 30, 300)
    ]


def test_rebuild_period_candles_is_idempotent_and_recomputes() -> None:
    theme_id, _ = _setup()
    with session_scope() as session:
        upsert_theme_daily_candles(
            session, [_theme_candle(theme_id, date(2026, 9, 28), "100", "100", "100", "100")]
        )
        rebuild_theme_period_candles(session, since=date(2026, 9, 28))
    with session_scope() as session:
        # 같은 날을 장중에 다시 계산해 종가가 바뀐 상황.
        upsert_theme_daily_candles(
            session, [_theme_candle(theme_id, date(2026, 9, 28), "100", "130", "100", "130")]
        )
        rebuild_theme_period_candles(session, since=date(2026, 9, 28))

    weekly = _period_rows(theme_id, "W")
    assert len(weekly) == 1
    assert weekly[0][4] == Decimal("130")


def test_run_daily_is_idempotent() -> None:
    theme_id, _ = _setup()
    _load_daily(
        [
            _daily(TICKERS[0], D1, "100"),
            _daily(TICKERS[1], D1, "200"),
            _daily(TICKERS[0], D2, "110"),
            _daily(TICKERS[1], D2, "180"),
            _daily(TICKERS[0], D3, "110"),
            _daily(TICKERS[1], D3, "216"),
        ]
    )

    first = run_daily(start=D1, end=D3, theme_ids=[theme_id])
    rows_first = _theme_rows(theme_id)
    run_daily(start=D1, end=D3, theme_ids=[theme_id])

    assert first == 2
    assert rows_first == [(D2, Decimal("1000.0000")), (D3, Decimal("1100.0000"))]
    assert _theme_rows(theme_id) == rows_first


def test_run_daily_split_windows_chain_through_anchor() -> None:
    theme_id, _ = _setup()
    _load_daily(
        [
            _daily(TICKERS[0], D1, "100"),
            _daily(TICKERS[1], D1, "200"),
            _daily(TICKERS[0], D2, "110"),
            _daily(TICKERS[1], D2, "180"),
            _daily(TICKERS[0], D3, "110"),
            _daily(TICKERS[1], D3, "216"),
        ]
    )

    run_daily(start=D1, end=D2, theme_ids=[theme_id])
    run_daily(start=D2, end=D3, theme_ids=[theme_id])  # D2 는 t-1 로만 쓰인다

    assert _theme_rows(theme_id) == [(D2, Decimal("1000.0000")), (D3, Decimal("1100.0000"))]


def test_run_daily_same_day_rerun_keeps_value() -> None:
    """같은 구간을 다시 계산해도 앵커가 같으니 값이 같다."""
    theme_id, _ = _setup()
    _load_daily([_daily(TICKERS[0], D1, "100"), _daily(TICKERS[0], D2, "105")])

    run_daily(start=D1, end=D2, theme_ids=[theme_id])
    run_daily(start=D1, end=D2, theme_ids=[theme_id])

    assert _theme_rows(theme_id) == [(D2, Decimal("1050.0000"))]


def test_run_period_uses_theme_daily_rows() -> None:
    theme_id, _ = _setup()
    _load_daily([_daily(TICKERS[0], D1, "100"), _daily(TICKERS[0], D2, "105")])
    run_daily(start=D1, end=D2, theme_ids=[theme_id])

    rows = run_period(since=D2, theme_ids=[theme_id])

    assert rows == 2
    assert _period_rows(theme_id, "M")[0][0] == date(2026, 9, 1)


def test_run_daily_isolates_theme_failure_and_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """한 테마가 실패해도 다른 테마는 적재되고, 끝에서 RuntimeError 로 태스크를 실패시킨다."""
    t1, stock_ids = _setup()
    with session_scope() as session:
        t2 = session.execute(
            text("INSERT INTO themes (name) VALUES (:n) RETURNING id"), {"n": THEME_NAME_2}
        ).scalar()
        session.execute(
            text("INSERT INTO theme_stocks (theme_id, stock_id) VALUES (:t, :s)"),
            [{"t": t2, "s": sid} for sid in stock_ids.values()],
        )
    _load_daily([_daily(TICKERS[0], D1, "100"), _daily(TICKERS[0], D2, "105")])

    from pipelines.themes.jobs import calculate_theme_candles as job

    original = job.compute_theme_candles

    def flaky(theme_id, *args, **kwargs):
        if theme_id == t2:
            raise RuntimeError("boom")
        return original(theme_id, *args, **kwargs)

    monkeypatch.setattr(job, "compute_theme_candles", flaky)

    with pytest.raises(RuntimeError):
        run_daily(start=D1, end=D2, theme_ids=[t1, t2])

    assert _theme_rows(t1) == [(D2, Decimal("1050.0000"))]
    assert _theme_rows(t2) == []


def _change_rates(table: str, date_column: str, theme_id: int, period: str | None = None) -> list:
    with session_scope() as session:
        rows = session.execute(
            text(
                f"SELECT {date_column}, change_rate FROM {table}"
                " WHERE theme_id = :t"
                + (" AND period = :p" if period else "")
                + f" ORDER BY {date_column}"
            ),
            {"t": theme_id, "p": period} if period else {"t": theme_id},
        )
        return [tuple(row) for row in rows]


def test_theme_daily_change_rate_is_vs_previous_bar() -> None:
    """지수 종가의 직전 봉 대비 %. 첫 봉은 NULL, 행이 빠진 날은 건너뛰고, 창을 나눠도 이어진다."""
    theme_id, _ = _setup()
    with session_scope() as session:
        upsert_theme_daily_candles(
            session,
            [
                _theme_candle(theme_id, D1, "1000", "1000", "1000", "1000"),
                _theme_candle(theme_id, D2, "1000", "1050", "1000", "1050"),
            ],
        )
        refresh_theme_change_rates(session, since=D1, theme_ids=[theme_id])
    with session_scope() as session:
        # D3 은 참여 종목이 없어 행이 없고, since 첫 행은 그 앞의 D2 와 비교한다.
        upsert_theme_daily_candles(
            session, [_theme_candle(theme_id, date(2026, 9, 4), "1050", "1050", "945", "945")]
        )
        refresh_theme_change_rates(session, since=date(2026, 9, 4), theme_ids=[theme_id])

    assert _change_rates("theme_candles_daily", "trade_date", theme_id) == [
        (D1, None),
        (D2, Decimal("5.00")),
        (date(2026, 9, 4), Decimal("-10.00")),
    ]


def test_run_daily_fills_change_rate_as_weighted_constituent_return() -> None:
    """등락률은 구성 종목 등락률의 가중 평균이다. 동일 가중 2종목이 D3 에 0%·+20% → +10%."""
    theme_id, _ = _setup()
    _load_daily(
        [
            _daily(TICKERS[0], D1, "100"),
            _daily(TICKERS[1], D1, "200"),
            _daily(TICKERS[0], D2, "110"),
            _daily(TICKERS[1], D2, "180"),
            _daily(TICKERS[0], D3, "110"),
            _daily(TICKERS[1], D3, "216"),
        ]
    )

    run_daily(start=D1, end=D3, theme_ids=[theme_id])
    changed = run_change_rates(since=D1, theme_ids=[theme_id])

    assert changed == 1
    assert _change_rates("theme_candles_daily", "trade_date", theme_id) == [
        (D2, None),
        (D3, Decimal("10.00")),
    ]


def test_theme_period_change_rate_is_vs_previous_period_bar() -> None:
    """주봉은 직전 주봉, 월봉은 직전 월봉 종가 대비다. 재집계 구간의 첫 봉도 그 앞 봉과 비교한다."""
    theme_id, _ = _setup()
    with session_scope() as session:
        upsert_theme_daily_candles(
            session,
            [
                _theme_candle(theme_id, date(2026, 8, 31), "100", "100", "100", "100"),
                _theme_candle(theme_id, date(2026, 9, 25), "90", "95", "85", "92"),
                _theme_candle(theme_id, date(2026, 9, 29), "106", "120", "101", "115"),
            ],
        )
        rebuild_theme_period_candles(session, since=date(2026, 8, 31), theme_ids=[theme_id])
        refresh_theme_change_rates(session, since=date(2026, 8, 31), theme_ids=[theme_id])
    with session_scope() as session:
        # 이번 주만 다시 계산해도 직전 주·월 봉과 비교하고 값이 그대로다.
        assert (
            refresh_theme_change_rates(session, since=date(2026, 9, 29), theme_ids=[theme_id]) == 0
        )

    assert _change_rates("theme_candles_period", "base_date", theme_id, "W") == [
        (date(2026, 8, 31), None),
        (date(2026, 9, 21), Decimal("-8.00")),
        (date(2026, 9, 28), Decimal("25.00")),
    ]
    assert _change_rates("theme_candles_period", "base_date", theme_id, "M") == [
        (date(2026, 8, 1), None),
        (date(2026, 9, 1), Decimal("15.00")),
    ]
