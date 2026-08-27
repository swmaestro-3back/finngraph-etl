"""테마 임베딩 로더(스테일 판정·적재) 통합 테스트.

로컬 DB 필요: docker compose up -d db 후 마이그레이션(0000) 적용 상태.

스테일 판정이 이 로더의 핵심 로직이다 — 해시는 SQL 에서 계산해 SELECT 로
가져오고 UPDATE 때 그대로 되쓰므로, 판정과 기록이 같은 표현식에서 나온다는
계약을 여기서 검증한다. (리프레시가 전량 삭제-재적재라 실전에서는 매 회차
전량이 스테일이고, 이 판정은 잡 중간 실패 시 재개 기준으로 쓰인다.)
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.themes.loaders.embeddings import (
    EMBEDDING_DIM,
    fetch_stale_reasons,
    fetch_stale_themes,
    update_reason_embeddings,
    update_theme_embeddings,
)

pytestmark = pytest.mark.integration


def _vector(fill: float = 0.5) -> list[float]:
    return [fill] * EMBEDDING_DIM


@pytest.fixture
def theme_row():
    name = f"통합테스트테마-{uuid.uuid4().hex[:8]}"
    with session_scope() as session:
        theme_id = session.execute(
            text(
                """
                INSERT INTO themes (name, description)
                VALUES (:name, '테스트 테마 설명')
                RETURNING id;
                """
            ),
            {"name": name},
        ).scalar_one()

    yield int(theme_id)

    with session_scope() as session:
        session.execute(text("DELETE FROM themes WHERE id = :id"), {"id": theme_id})


@pytest.fixture
def theme_stock_row(theme_row):
    ticker = uuid.uuid4().hex[:6].upper()
    with session_scope() as session:
        stock_id = session.execute(
            text(
                """
                INSERT INTO stocks (name, ticker, market, standard_code)
                VALUES (:name, :ticker, 'KOSPI', :standard_code)
                RETURNING id;
                """
            ),
            {
                "name": f"통합테스트종목-{ticker}",
                "ticker": ticker,
                "standard_code": f"KR{uuid.uuid4().hex[:10].upper()}",
            },
        ).scalar_one()
        link_id = session.execute(
            text(
                """
                INSERT INTO theme_stocks (theme_id, stock_id, reason)
                VALUES (:theme_id, :stock_id, 'ESS용 배터리 셀 생산')
                RETURNING id;
                """
            ),
            {"theme_id": theme_row, "stock_id": stock_id},
        ).scalar_one()

    yield int(link_id)

    with session_scope() as session:
        session.execute(text("DELETE FROM theme_stocks WHERE id = :id"), {"id": link_id})
        session.execute(text("DELETE FROM stocks WHERE id = :id"), {"id": stock_id})


def _stale_theme_ids(session) -> dict[int, dict]:
    return {row["id"]: row for row in fetch_stale_themes(session)}


def _stale_reason_ids(session) -> dict[int, dict]:
    return {row["id"]: row for row in fetch_stale_reasons(session)}


def test_new_theme_is_stale_with_name_and_description_text(theme_row):
    with session_scope() as session:
        stale = _stale_theme_ids(session)

    assert theme_row in stale
    row = stale[theme_row]
    assert "테스트 테마 설명" in row["text"]
    assert row["text_hash"]


def test_embedded_theme_with_unchanged_text_is_not_stale(theme_row):
    with session_scope() as session:
        row = _stale_theme_ids(session)[theme_row]
        update_theme_embeddings(
            session, [{"id": theme_row, "embedding": _vector(), "text_hash": row["text_hash"]}]
        )

    with session_scope() as session:
        assert theme_row not in _stale_theme_ids(session)


def test_description_change_makes_theme_stale_again(theme_row):
    with session_scope() as session:
        row = _stale_theme_ids(session)[theme_row]
        update_theme_embeddings(
            session, [{"id": theme_row, "embedding": _vector(), "text_hash": row["text_hash"]}]
        )
        session.execute(
            text("UPDATE themes SET description = '바뀐 설명' WHERE id = :id"),
            {"id": theme_row},
        )

    with session_scope() as session:
        assert theme_row in _stale_theme_ids(session)


def test_stored_embedding_roundtrips_with_dimension(theme_row):
    with session_scope() as session:
        row = _stale_theme_ids(session)[theme_row]
        update_theme_embeddings(
            session, [{"id": theme_row, "embedding": _vector(0.25), "text_hash": row["text_hash"]}]
        )

    with session_scope() as session:
        stored = session.execute(
            text("SELECT vector_dims(embedding) FROM themes WHERE id = :id"),
            {"id": theme_row},
        ).scalar_one()

    assert stored == EMBEDDING_DIM


def test_new_reason_is_stale(theme_stock_row):
    with session_scope() as session:
        stale = _stale_reason_ids(session)

    assert theme_stock_row in stale
    assert stale[theme_stock_row]["text"] == "ESS용 배터리 셀 생산"


def test_embedded_reason_with_unchanged_text_is_not_stale(theme_stock_row):
    with session_scope() as session:
        row = _stale_reason_ids(session)[theme_stock_row]
        update_reason_embeddings(
            session,
            [{"id": theme_stock_row, "embedding": _vector(), "text_hash": row["text_hash"]}],
        )

    with session_scope() as session:
        assert theme_stock_row not in _stale_reason_ids(session)


def test_null_reason_is_not_selected(theme_stock_row):
    with session_scope() as session:
        session.execute(
            text("UPDATE theme_stocks SET reason = NULL WHERE id = :id"),
            {"id": theme_stock_row},
        )

    with session_scope() as session:
        assert theme_stock_row not in _stale_reason_ids(session)
