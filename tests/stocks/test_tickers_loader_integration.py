"""stocks 로더 통합 테스트 — 종목코드 재사용·재등장 시나리오.

실제 Postgres에 붙어 `sync_tickers`의 upsert/비활성 SQL이 의도대로 동작하는지 검증한다.
`integration` 마커가 붙어 CI unit-test job에서는 제외되고 db-integration job에서만 실행된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

검증 대상은 두 가지다.

1. 종목코드 재사용 — 상장폐지된 종목과 같은 단축코드로 신규 상장이 들어와도 두 회사가
   서로 다른 행으로 공존해야 한다. 예전 구조(ticker PK)에서는 upsert가 옛 회사 행을
   덮어써서 이름·상장일이 새 회사 것으로 바뀌었다.

2. 일시 누락 후 재등장 — 하루 master에서 빠졌다가 다시 나타난 종목은 새 행이 아니라
   기존 행이 되살아나야 한다. conflict target을 standard_code로 둔 이유다.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.stocks.loaders.tickers import sync_tickers
from pipelines.stocks.models import StockTicker

pytestmark = pytest.mark.integration

# 운영 데이터와 섞이지 않도록 테스트 전용 시장 구분값. 정리(cleanup) 기준이기도 하다.
TEST_MARKET = "PYTEST_KOSPI"
TICKER = "999999"
OLD_CODE = "KR7999999001"
NEW_CODE = "KR7999999002"


def _make(standard_code: str, name: str, listed_on: date) -> StockTicker:
    return StockTicker(
        ticker=TICKER,
        standard_code=standard_code,
        name=name,
        market=TEST_MARKET,
        listed_date=listed_on,
        listed_shares=1_000_000,
    )


def _rows() -> list[dict]:
    with session_scope() as session:
        result = session.execute(
            text(
                """
                SELECT standard_code, name, ticker, is_active, listed_date
                  FROM stocks
                 WHERE market = :market
                 ORDER BY standard_code
                """
            ),
            {"market": TEST_MARKET},
        )
        return [dict(row._mapping) for row in result]


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            session.execute(
                text("DELETE FROM stocks WHERE market = :market"), {"market": TEST_MARKET}
            )

    purge()
    yield
    purge()


def test_reused_ticker_creates_separate_row() -> None:
    """상장폐지 후 같은 코드로 재상장되면 두 회사가 별개 행으로 남아야 한다."""
    old = _make(OLD_CODE, "옛회사", date(1990, 1, 1))
    with session_scope() as session:
        sync_tickers(session, [old])

    # 다음 날 master: 옛 종목이 사라지고 같은 단축코드로 새 종목이 등장
    new = _make(NEW_CODE, "새회사", date(2026, 8, 4))
    with session_scope() as session:
        sync_tickers(session, [new])

    rows = _rows()
    assert len(rows) == 2, f"두 회사가 공존해야 한다: {rows}"

    by_code = {row["standard_code"]: row for row in rows}
    assert by_code[OLD_CODE]["name"] == "옛회사"
    assert by_code[OLD_CODE]["is_active"] is False
    assert by_code[OLD_CODE]["listed_date"] == date(1990, 1, 1)

    assert by_code[NEW_CODE]["name"] == "새회사"
    assert by_code[NEW_CODE]["is_active"] is True

    # 활성 행은 하나뿐이어야 한다 (stocks_active_ticker_uk)
    assert sum(1 for row in rows if row["is_active"]) == 1


def test_missing_then_reappearing_ticker_reuses_row() -> None:
    """일시적으로 master에서 빠졌다 돌아온 종목은 새 행이 아니라 기존 행이 되살아난다."""
    ticker = _make(OLD_CODE, "종목A", date(2000, 5, 1))
    with session_scope() as session:
        sync_tickers(session, [ticker])

    # master에서 빠진 날 — 같은 시장의 다른 종목만 들어온다
    other = StockTicker(
        ticker="999998",
        standard_code="KR7999998003",
        name="다른종목",
        market=TEST_MARKET,
    )
    with session_scope() as session:
        sync_tickers(session, [other])

    assert {row["standard_code"]: row["is_active"] for row in _rows()}[OLD_CODE] is False

    # 다시 나타난 날
    with session_scope() as session:
        sync_tickers(session, [ticker, other])

    rows = [row for row in _rows() if row["standard_code"] == OLD_CODE]
    assert len(rows) == 1, "재등장 시 행이 늘어나면 안 된다"
    assert rows[0]["is_active"] is True
