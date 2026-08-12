"""search_keyword_repository 통합 테스트."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.database import session_scope

pytestmark = pytest.mark.integration

TEST_KEYWORD_PREFIX = "pytest-itest-kw-"

_MINIMAL_KEYWORDS_DDL = """
CREATE TABLE IF NOT EXISTS search_keywords (
    id               BIGSERIAL PRIMARY KEY,
    keyword          TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_searched_at TIMESTAMPTZ
);
"""

# 축소 이전 스키마가 남은 DB를 운영과 같은 조건으로 맞춘다.
_DROP_LEGACY_COLUMNS = """
ALTER TABLE search_keywords
    DROP COLUMN IF EXISTS source_type,
    DROP COLUMN IF EXISTS source_id,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS is_pinned;
"""


@pytest.fixture()
def repo():
    # conftest가 env를 세팅한 뒤 import되도록 함수 안에서 import한다.
    from pipelines.news.loaders import search_keyword_repository

    return search_keyword_repository


def _delete_test_rows() -> None:
    with session_scope() as session:
        session.execute(
            text("DELETE FROM search_keywords WHERE keyword LIKE :prefix"),
            {"prefix": TEST_KEYWORD_PREFIX + "%"},
        )


@pytest.fixture()
def keywords_table():
    with session_scope() as session:
        session.execute(text(_MINIMAL_KEYWORDS_DDL))
        session.execute(text(_DROP_LEGACY_COLUMNS))

    _delete_test_rows()
    yield
    _delete_test_rows()


def _insert(*, suffix: str, searched: bool) -> int:
    with session_scope() as session:
        row = session.execute(
            text(
                """
                INSERT INTO search_keywords (keyword, last_searched_at)
                VALUES (:keyword, CASE WHEN :searched THEN now() ELSE NULL END)
                RETURNING id;
                """
            ),
            {"keyword": TEST_KEYWORD_PREFIX + suffix, "searched": searched},
        ).fetchone()
        return row[0]


def _fetch_all(repo) -> list[dict]:
    # 서술어 시드 등 다른 행이 있어도 테스트 행이 포함되도록 넉넉한 limit을 쓴다.
    return repo.fetch_active_search_keywords(limit=1_000_000)


def test_never_searched_keyword_precedes_recently_searched(keywords_table, repo):
    never = TEST_KEYWORD_PREFIX + "never"
    recent = TEST_KEYWORD_PREFIX + "recent"
    _insert(suffix="never", searched=False)
    _insert(suffix="recent", searched=True)

    keywords = [row["keyword"] for row in _fetch_all(repo)]

    assert keywords.index(never) < keywords.index(recent)


def test_limit_caps_returned_rows(keywords_table, repo):
    _insert(suffix="limit-a", searched=False)
    _insert(suffix="limit-b", searched=False)
    _insert(suffix="limit-c", searched=False)

    assert len(repo.fetch_active_search_keywords(limit=2)) == 2


def test_returned_rows_expose_id_and_keyword_only(keywords_table, repo):
    _insert(suffix="shape", searched=False)

    row = next(row for row in _fetch_all(repo) if row["keyword"] == TEST_KEYWORD_PREFIX + "shape")

    assert set(row) == {"id", "keyword"}
    assert isinstance(row["id"], int)


def test_mark_keywords_searched_updates_last_searched_at(keywords_table, repo):
    keyword_id = _insert(suffix="mark", searched=False)

    assert repo.mark_keywords_searched([keyword_id]) == 1

    with session_scope() as session:
        last_searched_at = session.execute(
            text("SELECT last_searched_at FROM search_keywords WHERE id = :id"),
            {"id": keyword_id},
        ).scalar()

    assert last_searched_at is not None
