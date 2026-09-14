"""테마 편입 기업 조회·search_history 간격 판정·갱신 통합 테스트.

로컬 DB 필요: docker compose up -d db 후 0000_schema.sql 적용 상태.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.search_history import (
    fetch_due_company_queries,
    mark_companies_searched,
)

pytestmark = pytest.mark.integration


def _now() -> datetime:
    return datetime.now(UTC)


@pytest.fixture
def theme_with_stocks():
    """테마 2 + 기업 1 + 종목 3 (같은 기업의 종목 2개가 두 테마에 나뉘어 편입, 하나는 company_id NULL)."""

    tag = uuid.uuid4().hex[:8]
    with session_scope() as session:
        company_id = session.execute(
            text(
                """
                INSERT INTO companies (name, ticker, is_listed, country)
                VALUES (:name, :ticker, true, 'KR') RETURNING id;
                """
            ),
            {"name": f"통합테스트법인{tag}", "ticker": f"T{tag[:5]}"},
        ).scalar_one()
        common_stock_id = session.execute(
            text(
                """
                INSERT INTO stocks (name, ticker, market, company_id, standard_code)
                VALUES (:name, :ticker, 'KOSDAQ', :company_id, :code) RETURNING id;
                """
            ),
            {
                "name": f"통합테스트종목{tag}",
                "ticker": f"T{tag[:5]}",
                "company_id": company_id,
                "code": f"KR{tag}A",
            },
        ).scalar_one()
        preferred_stock_id = session.execute(
            text(
                """
                INSERT INTO stocks (name, ticker, market, company_id, standard_code)
                VALUES (:name, :ticker, 'KOSDAQ', :company_id, :code) RETURNING id;
                """
            ),
            {
                "name": f"통합테스트종목{tag}우",
                "ticker": f"P{tag[:5]}",
                "company_id": company_id,
                "code": f"KR{tag}P",
            },
        ).scalar_one()
        orphan_stock_id = session.execute(
            text(
                """
                INSERT INTO stocks (name, ticker, market, standard_code)
                VALUES (:name, :ticker, 'KOSDAQ', :code) RETURNING id;
                """
            ),
            {"name": f"미이관종목{tag}", "ticker": f"U{tag[:5]}", "code": f"KR{tag}B"},
        ).scalar_one()
        theme_a = session.execute(
            text("INSERT INTO themes (name) VALUES (:name) RETURNING id;"),
            {"name": f"통합테스트테마A{tag}"},
        ).scalar_one()
        theme_b = session.execute(
            text("INSERT INTO themes (name) VALUES (:name) RETURNING id;"),
            {"name": f"통합테스트테마B{tag}"},
        ).scalar_one()
        for theme_id, stock_id in (
            (theme_a, common_stock_id),
            (theme_a, orphan_stock_id),
            (theme_b, preferred_stock_id),
        ):
            session.execute(
                text("INSERT INTO theme_stocks (theme_id, stock_id) VALUES (:t, :s);"),
                {"t": theme_id, "s": stock_id},
            )

    yield {
        "theme_ids": [int(theme_a), int(theme_b)],
        "company_id": int(company_id),
        "common_name": f"통합테스트종목{tag}",
    }

    with session_scope() as session:
        # themes 삭제가 theme_stocks 를, companies 삭제가 search_history 를 CASCADE 로 지운다
        session.execute(
            text("DELETE FROM themes WHERE id = ANY(:ids)"), {"ids": [theme_a, theme_b]}
        )
        session.execute(
            text("DELETE FROM stocks WHERE id = ANY(:ids)"),
            {"ids": [common_stock_id, preferred_stock_id, orphan_stock_id]},
        )
        session.execute(text("DELETE FROM companies WHERE id = :c"), {"c": company_id})


def _mine(batch, company_id):
    return [q for q in batch.queries if q.company_id == company_id]


def test_fetch_returns_never_searched_company_once_across_themes(theme_with_stocks):
    theme_ids = theme_with_stocks["theme_ids"]

    batch = fetch_due_company_queries(theme_ids, interval_hours=2, now=_now())

    assert batch.theme_ids == sorted(theme_ids)
    mine = _mine(batch, theme_with_stocks["company_id"])
    # 두 테마·두 종목으로 나와도 기업 하나 = 쿼리 하나, 종목명은 stock id 순 첫 종목
    assert len(mine) == 1
    assert mine[0].name == theme_with_stocks["common_name"]
    assert mine[0].watermark is None
    # company_id NULL 종목은 쿼리에 없고 카운트된다
    assert batch.skipped_no_company == 1
    assert all("미이관종목" not in q.name for q in batch.queries)


def test_fetch_with_empty_theme_ids_hits_nothing(theme_with_stocks):
    batch = fetch_due_company_queries([], interval_hours=2, now=_now())

    assert batch.theme_ids == []
    assert batch.queries == []


def test_marked_company_is_not_due_until_interval_passes(theme_with_stocks):
    theme_ids = theme_with_stocks["theme_ids"]
    company_id = theme_with_stocks["company_id"]

    # 첫 마킹은 삽입, 두 번째는 갱신 — 둘 다 1행
    assert mark_companies_searched([company_id], _now()) == 1
    assert mark_companies_searched([company_id], _now()) == 1

    later = fetch_due_company_queries(theme_ids, interval_hours=2, now=_now())
    assert _mine(later, company_id) == []
    assert later.skipped_not_due == 1
    # 간격 0 시간이면 방금 마킹한 기업도 다시 대상이다
    assert len(_mine(fetch_due_company_queries(theme_ids, 0, _now()), company_id)) == 1


def test_due_predicate_uses_plain_interval(theme_with_stocks):
    theme_ids = theme_with_stocks["theme_ids"]
    company_id = theme_with_stocks["company_id"]

    # 2시간 5분 전에 시작한 런이 마킹한 기업은 2시간 간격에서 다시 대상이다
    mark_companies_searched([company_id], _now() - timedelta(hours=2, minutes=5))
    assert _mine(fetch_due_company_queries(theme_ids, 2, _now()), company_id)

    # 1시간 55분 전에 마킹한 기업은 아직 대상이 아니다 (허용치 없음)
    mark_companies_searched([company_id], _now() - timedelta(hours=1, minutes=55))
    assert not _mine(fetch_due_company_queries(theme_ids, 2, _now()), company_id)


def test_watermark_follows_last_searched_at(theme_with_stocks):
    theme_ids = theme_with_stocks["theme_ids"]
    company_id = theme_with_stocks["company_id"]
    searched_at = _now() - timedelta(hours=5)

    mark_companies_searched([company_id], searched_at)

    mine = _mine(fetch_due_company_queries(theme_ids, 2, _now()), company_id)
    assert mine[0].watermark is not None
    assert mine[0].watermark.tzinfo is not None  # TIMESTAMPTZ 라 aware 로 온다
    assert abs(mine[0].watermark - searched_at) < timedelta(seconds=1)


def test_mark_with_empty_list_touches_nothing():
    assert mark_companies_searched([], _now()) == 0
