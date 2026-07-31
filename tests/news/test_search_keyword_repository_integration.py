"""search_keyword_repository 통합 테스트.

실제 Postgres에 붙어 `fetch_active_search_keywords`가 is_pinned 키워드를
로테이션/배치 제한(limit)과 무관하게 항상 반환하는지 검증한다. `integration`
마커가 붙어 CI unit-test job에서는 제외되고 db-integration job에서만 실행된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

주의: 로더가 사용하는 컬럼만 담은 **테스트 로컬 최소 스키마**를 직접 생성/보정한다.
운영 스키마는 migrations/versions/20260725_04 + 20260729_07 에 있다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.database import session_scope

pytestmark = pytest.mark.integration

# 다른 데이터와 섞이지 않도록 테스트 전용 키워드 접두어. 정리(cleanup) 기준이기도 하다.
TEST_KEYWORD_PREFIX = "pytest-itest-kw-"

# 로더가 참조하는 컬럼만 담은 최소 스키마(운영 DDL 아님). 기존 테이블에 is_pinned가
# 없을 수 있으므로 ADD COLUMN IF NOT EXISTS로 보정한다.
_MINIMAL_KEYWORDS_DDL = """
CREATE TABLE IF NOT EXISTS search_keywords (
    id               BIGSERIAL PRIMARY KEY,
    keyword          TEXT NOT NULL UNIQUE,
    source_type      VARCHAR(20),
    source_id        BIGINT,
    status           VARCHAR(20) NOT NULL DEFAULT 'active',
    is_pinned        BOOLEAN NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_searched_at TIMESTAMPTZ
);
"""
_ADD_IS_PINNED = (
    "ALTER TABLE search_keywords ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT false;"
)


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
        session.execute(text(_ADD_IS_PINNED))

    _delete_test_rows()
    yield
    _delete_test_rows()


def _insert(*, suffix, is_pinned, status="active") -> int:
    # last_searched_at을 now()로 채워 "방금 검색됨"으로 둔다. 로테이션 우선순위에서
    # 뒤로 밀리므로, pinned가 아니면 작은 limit에서 뽑히지 않아야 한다.
    with session_scope() as session:
        row = session.execute(
            text(
                """
                INSERT INTO search_keywords
                    (keyword, source_type, status, is_pinned, last_searched_at)
                VALUES (:keyword, NULL, :status, :is_pinned, now())
                RETURNING id;
                """
            ),
            {
                "keyword": TEST_KEYWORD_PREFIX + suffix,
                "status": status,
                "is_pinned": is_pinned,
            },
        ).fetchone()
        return row[0]


def _fetched_test_keywords(repo, *, limit):
    rows = repo.fetch_active_search_keywords(limit=limit)
    return {r["keyword"] for r in rows if r["keyword"].startswith(TEST_KEYWORD_PREFIX)}


def test_pinned_keyword_included_regardless_of_limit(keywords_table, repo):
    pinned = TEST_KEYWORD_PREFIX + "pinned"
    rotation = TEST_KEYWORD_PREFIX + "rotation"
    _insert(suffix="pinned", is_pinned=True)
    _insert(suffix="rotation", is_pinned=False)

    # limit=0: 로테이션 서브쿼리는 0건 → pinned만 반환되어야 한다.
    only_pinned = _fetched_test_keywords(repo, limit=0)
    assert pinned in only_pinned
    assert rotation not in only_pinned

    # limit이 충분하면 로테이션 키워드도 함께 나온다(단, DB에 다른 active 행이 많으면
    # rotation이 밀릴 수 있으므로 넉넉한 limit으로 검증).
    both = _fetched_test_keywords(repo, limit=1_000_000)
    assert pinned in both
    assert rotation in both


def test_paused_pinned_keyword_is_excluded(keywords_table, repo):
    # is_pinned=true여도 status가 active가 아니면 조회되지 않아야 한다.
    paused = TEST_KEYWORD_PREFIX + "paused-pinned"
    _insert(suffix="paused-pinned", is_pinned=True, status="paused")

    assert paused not in _fetched_test_keywords(repo, limit=1_000_000)
