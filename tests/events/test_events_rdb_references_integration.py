"""events RDB 참조 조회 통합 테스트.

실제 Postgres 에 news / news_clusters 행을 만들고 조회 함수를 직접 호출한다.
선례: tests/news/test_news_clusters_repository_integration.py (uuid 마커 + session_scope 정리).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.references.rdb import (
    fetch_cluster_members,
    fetch_cluster_news_ids,
    fetch_promotable_clusters,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)


def _insert_cluster(
    session,
    *,
    original_size: int,
    member_count: int,
    last_offset_hours: int,
    updated_at: datetime | None = None,
) -> int:
    cluster_id = session.execute(
        text(
            """
            INSERT INTO news_clusters (
                keywords, original_size, member_count, first_published_at, last_published_at
            )
            VALUES (CAST(:keywords AS text[]), :original_size, :member_count, :first, :last)
            RETURNING id;
            """
        ),
        {
            "keywords": ["삼성전자", "유상증자"],
            "original_size": original_size,
            "member_count": member_count,
            "first": PUBLISHED,
            "last": PUBLISHED + timedelta(hours=last_offset_hours),
        },
    ).scalar_one()
    if updated_at is not None:
        session.execute(
            text("UPDATE news_clusters SET updated_at = :u WHERE id = :id"),
            {"u": updated_at, "id": cluster_id},
        )
    return int(cluster_id)


def _insert_news(
    session,
    marker: str,
    index: int,
    cluster_id: int,
    *,
    summary: str | None,
    published_at: datetime | None,
) -> int:
    return int(
        session.execute(
            text(
                """
                INSERT INTO news (title, text, summary, link, published_at, cluster_id)
                VALUES (:title, :text, :summary, :link, :published_at, :cluster_id)
                RETURNING id;
                """
            ),
            {
                "title": f"기사 {marker} {index}",
                "text": f"본문 {index} " * 3,
                "summary": summary,
                "link": f"https://example.com/ev/{marker}/{index}",
                "published_at": published_at,
                "cluster_id": cluster_id,
            },
        ).scalar_one()
    )


@pytest.fixture
def fixture_rows():
    """클러스터 4개: 승격 대상(hot, 원시 3, 최신), 스캔범위 밖(stale, 원시 2, 오래됨),
    미달(small, 원시 1), 멤버 0(empty, 원시 5)."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        hot = _insert_cluster(session, original_size=3, member_count=2, last_offset_hours=5)
        stale = _insert_cluster(
            session,
            original_size=2,
            member_count=1,
            last_offset_hours=1,
            updated_at=datetime(2020, 1, 1, tzinfo=SEOUL_TIMEZONE),
        )
        small = _insert_cluster(session, original_size=1, member_count=1, last_offset_hours=0)
        empty = _insert_cluster(session, original_size=5, member_count=0, last_offset_hours=0)

        # hot 멤버: 보도일 NULL 인 기사가 뒤로, 같은 날짜면 id 순
        n2 = _insert_news(session, marker, 2, hot, summary=None, published_at=None)
        n1 = _insert_news(session, marker, 1, hot, summary="요약 하나", published_at=PUBLISHED)
        n3 = _insert_news(session, marker, 3, hot, summary=None, published_at=PUBLISHED)
        s1 = _insert_news(session, marker, 4, stale, summary=None, published_at=PUBLISHED)
        m1 = _insert_news(session, marker, 5, small, summary=None, published_at=PUBLISHED)

    yield {
        "hot": hot,
        "stale": stale,
        "small": small,
        "empty": empty,
        "hot_news": (n1, n3, n2),
        "stale_news": s1,
        "small_news": m1,
    }

    with session_scope() as session:
        session.execute(
            text("DELETE FROM news WHERE link LIKE :p"), {"p": f"https://example.com/ev/{marker}/%"}
        )
        session.execute(
            text("DELETE FROM news_clusters WHERE id = ANY(:ids)"),
            {"ids": [hot, stale, small, empty]},
        )


def test_fetch_promotable_clusters_filters_and_orders(fixture_rows):
    since = datetime(2026, 1, 1, tzinfo=SEOUL_TIMEZONE)

    rows = fetch_promotable_clusters(min_size=2, since=since)
    ids = [row.cluster_id for row in rows]

    assert fixture_rows["hot"] in ids
    assert fixture_rows["stale"] not in ids  # updated_at 이 since 이전
    assert fixture_rows["small"] not in ids  # original_size 1
    assert fixture_rows["empty"] not in ids  # member_count 0

    hot = next(row for row in rows if row.cluster_id == fixture_rows["hot"])
    assert hot.original_size == 3
    assert hot.member_count == 2
    assert hot.keywords == ["삼성전자", "유상증자"]
    assert hot.representative_news_id is None
    assert hot.last_published_at == PUBLISHED + timedelta(hours=5)


def test_fetch_promotable_clusters_min_size(fixture_rows):
    since = datetime(2026, 1, 1, tzinfo=SEOUL_TIMEZONE)

    ids = [row.cluster_id for row in fetch_promotable_clusters(min_size=4, since=since)]

    assert fixture_rows["hot"] not in ids


def test_fetch_cluster_members_orders_by_published_then_id(fixture_rows):
    n1, n3, n2 = fixture_rows["hot_news"]

    members = fetch_cluster_members([fixture_rows["hot"], fixture_rows["small"]])

    assert [m.news_id for m in members[fixture_rows["hot"]]] == [n1, n3, n2]
    first = members[fixture_rows["hot"]][0]
    assert first.summary == "요약 하나"
    assert first.text.startswith("본문 1")
    assert first.published_at == PUBLISHED
    assert members[fixture_rows["hot"]][2].published_at is None
    assert [m.news_id for m in members[fixture_rows["small"]]] == [fixture_rows["small_news"]]


def test_fetch_cluster_news_ids(fixture_rows):
    n1, n3, n2 = fixture_rows["hot_news"]

    news_ids = fetch_cluster_news_ids([fixture_rows["hot"], fixture_rows["empty"]])

    assert news_ids[fixture_rows["hot"]] == sorted([n1, n2, n3])
    assert fixture_rows["empty"] not in news_ids


def test_empty_inputs():
    assert fetch_cluster_members([]) == {}
    assert fetch_cluster_news_ids([]) == {}
