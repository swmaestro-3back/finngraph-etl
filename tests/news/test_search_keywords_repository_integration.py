"""search_keywords 테이블 기반 검색 쿼리 조회·마킹 통합 테스트.

로컬 DB 필요: docker compose up -d db 후 마이그레이션 적용 상태.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.loaders.postgres import fetch_search_keywords, mark_keywords_searched

pytestmark = pytest.mark.integration


@pytest.fixture
def keyword_row():
    keyword = f"통합테스트,{uuid.uuid4().hex[:8]}"
    with session_scope() as session:
        keyword_id = session.execute(
            text(
                """
                INSERT INTO search_keywords (keyword)
                VALUES (:keyword)
                RETURNING id;
                """
            ),
            {"keyword": keyword},
        ).scalar_one()

    yield int(keyword_id), keyword

    with session_scope() as session:
        session.execute(text("DELETE FROM search_keywords WHERE id = :id"), {"id": keyword_id})


def test_fetch_search_keywords_returns_inserted_row(keyword_row):
    keyword_id, keyword = keyword_row

    rows = fetch_search_keywords()

    assert {"id": keyword_id, "keyword": keyword} in rows


def test_initial_seed_keywords_exist():
    # 0000 베이스라인이 시드한 기본 검색 쿼리
    keywords = {row["keyword"] for row in fetch_search_keywords()}

    assert {"특징주,공급", "특징주,계약"} <= keywords


def test_mark_keywords_searched_updates_timestamp(keyword_row):
    keyword_id, _ = keyword_row

    assert mark_keywords_searched([keyword_id]) == 1

    with session_scope() as session:
        last_searched_at = session.execute(
            text("SELECT last_searched_at FROM search_keywords WHERE id = :id"),
            {"id": keyword_id},
        ).scalar_one()

    assert last_searched_at is not None
