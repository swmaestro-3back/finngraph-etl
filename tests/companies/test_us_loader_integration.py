"""US 로더 통합 테스트 — companies·stocks·aliases 분리 적재와 멱등.

로컬 Postgres 필요: `docker compose up -d db` 후 `pytest -m integration`.
테스트 종목은 PYTEST_ 접두 티커를 써서 실데이터와 겹치지 않는다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.companies.loaders.postgres import sync_listed_companies
from pipelines.companies.loaders.us import (
    STOCK_SOURCE_WIKIPEDIA,
    clean_company_name,
    load_us_companies,
)
from pipelines.companies.models import UsCompany

pytestmark = pytest.mark.integration

TICKER = "PYTESTUS"


def _company(**overrides) -> UsCompany:
    base = dict(
        ticker=TICKER,
        market="NASDAQ",
        name="파이테스트US",
        name_eng="Pytest US Inc.",
        description="설명",
        ceo_name="CEO",
        homepage="https://pytest.example.com",
        raw_attributes={
            "name_eng": "Pytest US Inc.",
            "employees": 10,
            "reuters_code": "PYTESTUS.O",
        },
    )
    return UsCompany(**{**base, **overrides})


def _load(*companies: UsCompany):
    with session_scope() as session:
        return load_us_companies(session, list(companies))


def _rows(sql: str) -> list[dict]:
    with session_scope() as session:
        return [dict(r._mapping) for r in session.execute(text(sql), {"tickers": [TICKER]})]


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            session.execute(text("DELETE FROM stocks WHERE ticker = ANY(:t)"), {"t": [TICKER]})
            session.execute(
                text("DELETE FROM companies WHERE ticker = ANY(:t) AND country = 'US'"),
                {"t": [TICKER]},
            )

    purge()
    yield
    purge()


def test_splits_into_companies_stocks_and_aliases() -> None:
    result = _load(_company())

    assert (result.company_count, result.stock_count, result.alias_count) == (1, 1, 1)

    [company] = _rows(
        "SELECT name, country, is_listed, description, description_source, ceo_name, homepage "
        "FROM companies WHERE ticker = ANY(:tickers)"
    )
    assert company == {
        "name": "파이테스트US",
        "country": "US",
        "is_listed": True,
        "description": "설명",
        "description_source": "NAVER",
        "ceo_name": "CEO",
        "homepage": "https://pytest.example.com",
    }

    [stock] = _rows(
        "SELECT s.name, s.market, s.standard_code, s.source, s.is_active, s.raw_attributes, "
        "c.ticker AS company_ticker "
        "FROM stocks s JOIN companies c ON c.id = s.company_id WHERE s.ticker = ANY(:tickers)"
    )
    assert stock["name"] == "파이테스트US"
    assert stock["market"] == "NASDAQ"
    assert stock["standard_code"] is None  # US는 표준코드가 없다
    assert stock["source"] == STOCK_SOURCE_WIKIPEDIA
    assert stock["is_active"] is True
    assert stock["raw_attributes"]["name_eng"] == "Pytest US Inc."
    assert stock["raw_attributes"]["reuters_code"] == "PYTESTUS.O"
    assert stock["company_ticker"] == TICKER


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("메타 플랫폼스(페이스북)", "메타플랫폼스"),
        ("에이알엠 홀딩스(ADR)", "에이알엠홀딩스"),
        ("ASML 홀딩(ADR)", "ASML홀딩"),
        ("알파벳 A", "알파벳A"),
        ("핀둬둬（ADR）", "핀둬둬"),
        ("애플", "애플"),
    ],
)
def test_clean_company_name(name: str, expected: str) -> None:
    assert clean_company_name(name) == expected


def test_companies_name_drops_parentheses_and_spaces_but_stocks_name_is_raw() -> None:
    _load(_company(name="파이 테스트US(ADR)"))

    [company] = _rows("SELECT name FROM companies WHERE ticker = ANY(:tickers)")
    assert company == {"name": "파이테스트US"}
    [stock] = _rows("SELECT name FROM stocks WHERE ticker = ANY(:tickers)")
    assert stock == {"name": "파이 테스트US(ADR)"}


def test_rerun_replaces_legacy_standard_code_and_source() -> None:
    """예전 회차가 남긴 reutersCode·US_INDEX 행도 같은 티커로 갱신돼 NULL·WIKIPEDIA가 된다."""

    _load(_company())
    with session_scope() as session:
        session.execute(
            text(
                "UPDATE stocks SET standard_code = 'PYTESTUS.O', source = 'US_INDEX' "
                "WHERE ticker = ANY(:t)"
            ),
            {"t": [TICKER]},
        )

    _load(_company())

    assert _rows("SELECT standard_code, source FROM stocks WHERE ticker = ANY(:tickers)") == [
        {"standard_code": None, "source": STOCK_SOURCE_WIKIPEDIA}
    ]

    aliases = _rows(
        "SELECT a.alias, a.lang, a.source FROM company_aliases a "
        "JOIN companies c ON c.id = a.company_id WHERE c.ticker = ANY(:tickers)"
    )
    assert aliases == [{"alias": "Pytest US Inc.", "lang": "en", "source": "WIKIPEDIA"}]


def test_rerun_is_idempotent_and_missing_overview_keeps_old_values() -> None:
    _load(_company())

    # 두 번째 주: 네이버 실패로 description·ceo_name이 None
    result = _load(
        _company(
            description=None,
            ceo_name=None,
            raw_attributes={"name_eng": "Pytest US Inc."},
        )
    )

    assert result.alias_count == 0
    [company] = _rows("SELECT description, ceo_name FROM companies WHERE ticker = ANY(:tickers)")
    assert company == {"description": "설명", "ceo_name": "CEO"}
    assert len(_rows("SELECT id FROM stocks WHERE ticker = ANY(:tickers)")) == 1
    [stock] = _rows("SELECT raw_attributes FROM stocks WHERE ticker = ANY(:tickers)")
    assert stock["raw_attributes"]["employees"] == 10  # JSONB 병합으로 보존


def test_kr_alias_sync_does_not_touch_us_stocks() -> None:
    _load(_company())

    with session_scope() as session:
        sync_listed_companies(session)

    aliases = _rows(
        "SELECT a.source FROM company_aliases a JOIN companies c ON c.id = a.company_id "
        "WHERE c.ticker = ANY(:tickers)"
    )
    assert {a["source"] for a in aliases} == {"WIKIPEDIA"}


OTHER_TICKER = "PYTESTUSB"


def test_ticker_dropped_from_index_stays_active() -> None:
    """지수에서 빠진 티커는 비활성화 대상이 아니다 — 계속 active·listed로 남는다."""

    a = _company()
    b = _company(
        ticker=OTHER_TICKER,
        name="파이테스트US비",
        name_eng="Pytest US B Inc.",
        raw_attributes={"name_eng": "Pytest US B Inc."},
    )

    try:
        _load(a, b)
        # 다음 회차: 이번엔 A만 있다 (B는 지수에서 빠짐)
        _load(a)

        with session_scope() as session:
            rows = session.execute(
                text("SELECT ticker, is_active FROM stocks WHERE ticker = ANY(:t) ORDER BY ticker"),
                {"t": [TICKER, OTHER_TICKER]},
            ).all()
        assert [(r.ticker, r.is_active) for r in rows] == [(TICKER, True), (OTHER_TICKER, True)]
    finally:
        with session_scope() as session:
            session.execute(text("DELETE FROM stocks WHERE ticker = :t"), {"t": OTHER_TICKER})
            session.execute(
                text("DELETE FROM companies WHERE ticker = :t AND country = 'US'"),
                {"t": OTHER_TICKER},
            )
