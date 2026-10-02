"""개체 사전 적재 통합 테스트 — 수동 별칭 ticker 시드, 전량 교체, 축소 가드.

로컬 Postgres 필요: `docker compose up -d db` 후 V7 적용, `pytest -m integration`.
테스트 종목은 PYTESTGZ 티커로 직접 만든다. entity_gazetteer 는 전량 교체 테이블이라
교체 테스트 전후로 원래 행을 보존·복원한다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.companies.loaders.gazetteer import (
    INSERT_GAZETTEER_SQL,
    replace_gazetteer,
    seed_curated_aliases_by_ticker,
)
from pipelines.companies.models import GazetteerAlias

pytestmark = pytest.mark.integration

TICKER = "PYTESTGZ"


def _purge() -> None:
    with session_scope() as session:
        session.execute(text("DELETE FROM stocks WHERE ticker = :t"), {"t": TICKER})
        session.execute(text("DELETE FROM companies WHERE ticker = :t"), {"t": TICKER})


@pytest.fixture
def listed() -> tuple[int, int]:
    """PYTESTGZ 상장 기업과 활성 종목을 만들고 (company_id, stock_id)를 돌려준다."""

    _purge()
    with session_scope() as session:
        company_id = session.execute(
            text(
                "INSERT INTO companies (name, ticker, is_listed, country) "
                "VALUES ('파이테스트지제티어', :t, true, 'US') RETURNING id"
            ),
            {"t": TICKER},
        ).scalar_one()
        stock_id = session.execute(
            text(
                "INSERT INTO stocks (name, ticker, market, company_id, source) "
                "VALUES ('파이테스트 지제티어', :t, 'NASDAQ', :company_id, 'WIKIPEDIA') RETURNING id"
            ),
            {"t": TICKER, "company_id": company_id},
        ).scalar_one()
    yield company_id, stock_id
    _purge()


@pytest.fixture
def preserved_gazetteer():
    with session_scope() as session:
        rows = session.execute(
            text(
                "SELECT alias, company_id, stock_id, ticker, canonical_name, "
                "alias_source AS source FROM entity_gazetteer"
            )
        ).mappings()
        saved = [dict(row) for row in rows]
    yield
    with session_scope() as session:
        session.execute(text("DELETE FROM entity_gazetteer"))
        if saved:
            session.execute(INSERT_GAZETTEER_SQL, saved)


def _aliases(company_id: int) -> list[tuple[str, str]]:
    with session_scope() as session:
        rows = session.execute(
            text("SELECT alias, source FROM company_aliases WHERE company_id = :id ORDER BY alias"),
            {"id": company_id},
        )
        return [tuple(row) for row in rows]


def _entries(company_id: int, stock_id: int, count: int) -> list[GazetteerAlias]:
    return [
        GazetteerAlias(
            alias=f"파이테스트별칭{index}",
            company_id=company_id,
            stock_id=stock_id,
            ticker=TICKER,
            canonical_name="파이테스트지제티어",
            source="CURATED",
        )
        for index in range(count)
    ]


def _gazetteer_aliases() -> list[str]:
    with session_scope() as session:
        return list(
            session.execute(text("SELECT alias FROM entity_gazetteer ORDER BY alias")).scalars()
        )


def test_curated_seed_by_ticker_is_idempotent(listed):
    company_id, _ = listed
    seed = [("파테지", TICKER), ("없는종목", "PYTESTNONE")]

    with session_scope() as session:
        assert seed_curated_aliases_by_ticker(session, seed) == 1
    with session_scope() as session:
        assert seed_curated_aliases_by_ticker(session, seed) == 0

    assert _aliases(company_id) == [("파테지", "CURATED")]


def test_curated_seed_promotes_wikipedia_alias(listed):
    company_id, _ = listed
    with session_scope() as session:
        session.execute(
            text(
                "INSERT INTO company_aliases (company_id, alias, lang, source) "
                "VALUES (:id, 'PytestGZ', 'en', 'WIKIPEDIA')"
            ),
            {"id": company_id},
        )
        seed_curated_aliases_by_ticker(session, [("PytestGZ", TICKER)])

    assert _aliases(company_id) == [("PytestGZ", "CURATED")]


def test_replace_gazetteer_swaps_whole_table(listed, preserved_gazetteer):
    with session_scope() as session:
        session.execute(text("DELETE FROM entity_gazetteer"))
        replace_gazetteer(session, _entries(*listed, count=2))
    with session_scope() as session:
        assert replace_gazetteer(session, _entries(*listed, count=1)) == 2

    assert _gazetteer_aliases() == ["파이테스트별칭0"]


def test_replace_gazetteer_refuses_to_shrink_below_half(listed, preserved_gazetteer):
    with session_scope() as session:
        session.execute(text("DELETE FROM entity_gazetteer"))
        replace_gazetteer(session, _entries(*listed, count=4))

    with pytest.raises(RuntimeError, match="교체하지 않는다"), session_scope() as session:
        replace_gazetteer(session, _entries(*listed, count=1))

    assert len(_gazetteer_aliases()) == 4
