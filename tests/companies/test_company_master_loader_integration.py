"""companies 로더 통합 테스트 — 법인 적재·종목 연결·별칭.

실제 Postgres에 붙어 `sync_listed_companies`의 세 SQL이 의도대로 동작하는지 검증한다.
`integration` 마커가 붙어 CI unit-test job에서는 제외되고 db-integration job에서만 실행된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

검증 대상은 세 가지다.

1. 보통주는 법인이 된다 — companies에 행이 생기고 stocks.company_id가 연결되며
   종목명과 단축코드가 모두 별칭으로 들어간다.

2. 우선주·ETP·SPAC는 법인이 아니다 — 삼성전자우는 삼성전자와 같은 법인이고 ETF는
   애초에 사업을 하는 법인이 아니다. companies에 들어가지 않고 company_id도 NULL로 남는다.

3. 재실행이 멱등이다 — 매일 도는 job이라 두 번째 실행에서 법인·별칭이 늘거나
   연결이 다시 갱신되면 안 된다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.companies.loaders.companies import sync_listed_companies
from pipelines.stocks.loaders.tickers import sync_tickers
from pipelines.stocks.models import StockTicker

pytestmark = pytest.mark.integration

# 운영 데이터와 섞이지 않도록 stocks는 테스트 전용 시장 구분값으로 격리한다.
# companies에는 market이 없으므로(시장은 종목 속성) 테스트 ticker 집합으로 식별·정리한다.
TEST_MARKET = "PYTEST_COMPANIES"

COMMON_TICKER = "999901"
PREFERRED_TICKER = "999902"
ETP_TICKER = "999903"
TEST_TICKERS = [COMMON_TICKER, PREFERRED_TICKER, ETP_TICKER]


def _stock(ticker: str, name: str, **flags: bool) -> StockTicker:
    return StockTicker(
        ticker=ticker,
        standard_code=f"KR7{ticker}001",
        name=name,
        market=TEST_MARKET,
        **flags,
    )


def _load_stocks(*tickers: StockTicker) -> None:
    with session_scope() as session:
        sync_tickers(session, list(tickers))


def _sync() -> tuple[int, int, int]:
    with session_scope() as session:
        result = sync_listed_companies(session)
    return result.upserted_count, result.linked_count, result.alias_count


def _companies() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT ticker, name, country, is_listed
                  FROM companies
                 WHERE ticker = ANY(:tickers)
                 ORDER BY ticker
                """
            ),
            {"tickers": TEST_TICKERS},
        )
        return [dict(row._mapping) for row in rows]


def _stock_links() -> dict[str, int | None]:
    with session_scope() as session:
        rows = session.execute(
            text("SELECT ticker, company_id FROM stocks WHERE market = :market"),
            {"market": TEST_MARKET},
        )
        return {row.ticker: row.company_id for row in rows}


def _aliases() -> set[str]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT a.alias
                  FROM company_aliases AS a
                  JOIN companies AS c ON c.id = a.company_id
                 WHERE c.ticker = ANY(:tickers)
                """
            ),
            {"tickers": TEST_TICKERS},
        )
        return {row.alias for row in rows}


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            # stocks가 companies를 참조하므로 종목을 먼저 지운다.
            # company_aliases는 ON DELETE CASCADE라 companies와 함께 사라진다.
            session.execute(
                text("DELETE FROM stocks WHERE market = :market"), {"market": TEST_MARKET}
            )
            session.execute(
                text("DELETE FROM companies WHERE ticker = ANY(:tickers)"),
                {"tickers": TEST_TICKERS},
            )

    purge()
    yield
    purge()


def test_common_stock_becomes_company_with_aliases() -> None:
    """보통주는 법인이 되고, 종목·별칭이 함께 연결된다."""
    _load_stocks(_stock(COMMON_TICKER, "테스트전자"))

    _sync()

    companies = _companies()
    assert len(companies) == 1
    assert companies[0]["ticker"] == COMMON_TICKER
    assert companies[0]["name"] == "테스트전자"
    assert companies[0]["country"] == "KR"
    assert companies[0]["is_listed"] is True

    assert _stock_links()[COMMON_TICKER] is not None
    assert _aliases() == {"테스트전자", COMMON_TICKER}


def test_preferred_and_etp_are_not_companies() -> None:
    """우선주·ETP는 법인이 아니므로 companies에 들어가지 않고 company_id도 비어 있다."""
    _load_stocks(
        _stock(COMMON_TICKER, "테스트전자"),
        _stock(PREFERRED_TICKER, "테스트전자우", preferred_stock=True),
        _stock(ETP_TICKER, "테스트ETF", etp=True),
    )

    _sync()

    assert [row["ticker"] for row in _companies()] == [COMMON_TICKER], (
        "보통주 한 종목만 법인이 되어야 한다"
    )

    links = _stock_links()
    assert links[COMMON_TICKER] is not None
    assert links[PREFERRED_TICKER] is None
    assert links[ETP_TICKER] is None

    # 법인에 연결되지 않은 종목의 이름은 별칭으로도 들어가지 않는다.
    assert "테스트전자우" not in _aliases()


def test_rerun_is_idempotent() -> None:
    """매일 도는 job이므로 두 번째 실행에서 법인·별칭이 늘지 않아야 한다."""
    _load_stocks(_stock(COMMON_TICKER, "테스트전자"))
    _sync()

    _, linked, alias_count = _sync()

    # linked/alias는 전 종목 기준 카운트다. 첫 실행에서 모두 연결·적재됐으므로
    # 두 번째 실행에서는 0이어야 한다. upsert는 매번 전 행을 갱신하므로 세지 않는다.
    assert linked == 0, "이미 같은 법인을 가리키므로 다시 연결하지 않는다"
    assert alias_count == 0, "이미 있는 별칭은 추가되지 않는다"

    assert len(_companies()) == 1
    assert _aliases() == {"테스트전자", COMMON_TICKER}
