"""theme_changes repository 통합 테스트 — 최신 거래일 기준 테마 등락률.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.theme_changes import fetch_theme_changes

pytestmark = pytest.mark.integration

# 실제 데이터와 겹치지 않도록 먼 미래 날짜를 쓴다
D0 = date(2099, 1, 5)  # 직전 거래일
D1 = date(2099, 1, 7)  # 기준일 (1/6 은 휴장으로 봉 없음)


@pytest.fixture
def theme_rows():
    """테마 1개 + 종목 3개. 종가 100→110, 100→90, 100→100 → 평균 0%. 4번째 종목은 D1 봉이 없다."""

    tag = uuid.uuid4().hex[:8]
    closes = [(100, 110), (100, 90), (100, 100), (100, None)]
    with session_scope() as session:
        theme_id = session.execute(
            text("INSERT INTO themes (name) VALUES (:n) RETURNING id;"), {"n": f"등락테스트{tag}"}
        ).scalar_one()
        stock_ids = []
        for i, (c0, c1) in enumerate(closes):
            stock_id = session.execute(
                text(
                    "INSERT INTO stocks (name, ticker, market, standard_code) "
                    "VALUES (:n, :t, 'KOSDAQ', :code) RETURNING id;"
                ),
                {"n": f"등락종목{tag}{i}", "t": f"C{tag[:4]}{i}", "code": f"KR{tag}{i}"},
            ).scalar_one()
            stock_ids.append(stock_id)
            session.execute(
                text("INSERT INTO theme_stocks (theme_id, stock_id) VALUES (:t, :s);"),
                {"t": theme_id, "s": stock_id},
            )
            for trade_date, close in ((D0, c0), (D1, c1)):
                if close is None:
                    continue
                session.execute(
                    text(
                        "INSERT INTO stock_candles_daily "
                        "(stock_id, trade_date, open, high, low, close, volume, source) "
                        "VALUES (:s, :d, :c, :c, :c, :c, 1, 'TEST');"
                    ),
                    {"s": stock_id, "d": trade_date, "c": close},
                )

    yield {"theme_id": int(theme_id)}

    with session_scope() as session:
        session.execute(text("DELETE FROM themes WHERE id = :t"), {"t": theme_id})
        # stocks 삭제가 stock_candles_daily 를 CASCADE 로 지운다
        session.execute(text("DELETE FROM stocks WHERE id = ANY(:ids)"), {"ids": stock_ids})


def _mine(rows, theme_id):
    return [r for r in rows if r.theme_id == theme_id]


def test_change_is_mean_of_stock_changes_against_previous_trading_day(theme_rows):
    [row] = _mine(fetch_theme_changes(as_of=D1), theme_rows["theme_id"])

    assert row.trade_date == D1
    assert row.stock_count == 3  # D1 봉이 없는 종목은 빠진다
    assert row.change == pytest.approx((10 - 10 + 0) / 3)


def test_as_of_picks_latest_trading_day_on_or_before(theme_rows):
    # 1/6 은 봉이 없어 1/5 가 기준일이 되고, 1/5 는 직전 봉이 없어 등락률을 낼 수 없다
    assert _mine(fetch_theme_changes(as_of=D1 - timedelta(days=1)), theme_rows["theme_id"]) == []


def test_min_stocks_excludes_thin_themes(theme_rows):
    assert _mine(fetch_theme_changes(as_of=D1, min_stocks=4), theme_rows["theme_id"]) == []
    assert _mine(fetch_theme_changes(as_of=D1, min_stocks=3), theme_rows["theme_id"])
