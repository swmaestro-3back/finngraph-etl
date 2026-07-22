"""news 로더(material 판정 read/write) 통합 테스트.

실제 Postgres에 붙어 `fetch_unchecked_news_items` / `mark_news_material_checked`의
SQL이 의도대로 동작하는지 검증한다. `integration` 마커가 붙어 CI unit-test job에서는
제외되고 db-integration job에서만 실행된다. 로컬은 `docker compose up -d db` 후
`pytest -m integration`.

주의: 저장소에 권위 있는 news DDL 파일이 없어(=migrations/versions 비어 있음, initdb는
확장만 생성), 이 테스트는 로더가 사용하는 컬럼만 담은 **테스트 로컬 최소 스키마**를
직접 생성한다. 운영 스키마가 확정되면 그 DDL을 적용하도록 교체할 것.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration

# 다른 데이터와 섞이지 않도록 테스트 전용 링크 접두어. 정리(cleanup) 기준이기도 하다.
TEST_LINK_PREFIX = "http://pytest-itest.local/news/"

# 로더가 참조하는 컬럼만 담은 최소 스키마(운영 DDL 아님).
_MINIMAL_NEWS_DDL = """
CREATE TABLE IF NOT EXISTS news (
    id                  BIGSERIAL PRIMARY KEY,
    title               TEXT NOT NULL,
    description         TEXT,
    summary             TEXT,
    body_text           TEXT,
    link                TEXT UNIQUE,
    originallink        TEXT,
    published_at        TIMESTAMPTZ,
    material_checked_at TIMESTAMPTZ,
    is_material         BOOLEAN
);
"""


@pytest.fixture()
def repo():
    # conftest가 env를 세팅한 뒤 import되도록 함수 안에서 import한다.
    from pipelines.news.loaders import news_repository

    return news_repository


def _delete_test_rows(repo) -> None:
    conn = repo.get_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute("DELETE FROM news WHERE link LIKE %s", (TEST_LINK_PREFIX + "%",))
    finally:
        conn.close()


@pytest.fixture()
def news_table(repo):
    conn = repo.get_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(_MINIMAL_NEWS_DDL)
    finally:
        conn.close()

    _delete_test_rows(repo)
    yield
    _delete_test_rows(repo)


def _insert(repo, *, suffix, body_text, checked_at=None, is_material=None) -> int:
    link = TEST_LINK_PREFIX + suffix
    conn = repo.get_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO news
                    (title, description, body_text, link, originallink,
                     material_checked_at, is_material)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                ("제목 " + suffix, "설명", body_text, link, link, checked_at, is_material),
            )
            return cursor.fetchone()[0]
    finally:
        conn.close()


def _state(repo, ids):
    conn = repo.get_connection()
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, material_checked_at, is_material FROM news WHERE id = ANY(%s)",
                (list(ids),),
            )
            return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    finally:
        conn.close()


def _unchecked_test_ids(repo):
    items = repo.fetch_unchecked_news_items(limit=1_000_000)
    return {it["_news_id"] for it in items if it["link"].startswith(TEST_LINK_PREFIX)}


def test_fetch_unchecked_returns_only_unjudged_with_body(news_table, repo):
    keep = _insert(repo, suffix="a-unchecked", body_text="텍스트 A")
    drop = _insert(repo, suffix="b-unchecked", body_text="텍스트 B")
    _insert(repo, suffix="c-null-body", body_text=None)
    _insert(repo, suffix="d-blank-body", body_text="   ")
    _insert(
        repo,
        suffix="e-already-checked",
        body_text="E",
        checked_at=datetime.now(UTC),
        is_material=True,
    )

    # 미판정 + 텍스트 있음 인 a, b 만 조회되어야 한다.
    assert _unchecked_test_ids(repo) == {keep, drop}

    # 판정 결과 기록: a 유지, b 소프트삭제.
    result = repo.mark_news_material_checked(kept_ids=[keep], dropped_ids=[drop])
    assert result == {"kept_count": 1, "dropped_count": 1}

    # 기록 후에는 우리 테스트 행이 더 이상 미판정으로 조회되지 않는다.
    assert _unchecked_test_ids(repo) == set()

    # 실제 저장 상태 확인.
    state = _state(repo, [keep, drop])
    kept_checked_at, kept_is_material = state[keep]
    dropped_checked_at, dropped_is_material = state[drop]
    assert kept_checked_at is not None and kept_is_material is True
    assert dropped_checked_at is not None and dropped_is_material is False


def test_mark_with_empty_inputs_is_noop(news_table, repo):
    # 대상이 없으면 DB를 건드리지 않고 0/0을 돌려준다.
    assert repo.mark_news_material_checked([], []) == {"kept_count": 0, "dropped_count": 0}
