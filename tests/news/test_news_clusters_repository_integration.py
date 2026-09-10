"""news_clusters 로더 통합 테스트.

실제 Postgres 에 news / news_clusters 행을 만들고 로더 함수를 직접 호출한다. 0000 스키마가
적용된 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.loaders.clusters import (
    create_news_cluster,
    fetch_active_cluster_seeds,
    fetch_cluster_member_terms,
    update_news_cluster,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)


@pytest.fixture
def news_rows():
    """published_at 이 9/2, 9/3 인 뉴스 2건. 테스트가 만든 클러스터까지 함께 지운다."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        ids = [
            session.execute(
                text(
                    """
                    INSERT INTO news (title, text, link, published_at)
                    VALUES (:title, '본문', :link, :published_at)
                    RETURNING id;
                    """
                ),
                {
                    "title": f"클러스터 {marker} {i}",
                    "link": f"https://example.com/nc/{marker}/{i}",
                    "published_at": PUBLISHED + timedelta(days=i),
                },
            ).scalar_one()
            for i in range(2)
        ]

    yield ids

    with session_scope() as session:
        cluster_ids = [
            row[0]
            for row in session.execute(
                text("SELECT DISTINCT cluster_id FROM news WHERE id = ANY(:ids)"), {"ids": ids}
            ).fetchall()
            if row[0] is not None
        ]
        session.execute(text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": ids})
        if cluster_ids:
            session.execute(
                text("DELETE FROM news_clusters WHERE id = ANY(:ids)"), {"ids": cluster_ids}
            )


def _cluster_row(cluster_id: int) -> dict:
    with session_scope() as session:
        row = session.execute(
            text(
                """
                SELECT representative_news_id, keywords, term_weights, cohesion,
                       original_size, member_count, first_published_at, last_published_at
                FROM news_clusters
                WHERE id = :id;
                """
            ),
            {"id": cluster_id},
        ).one()
    return dict(row._mapping)


def _news_cluster_columns(news_id: int) -> tuple[int | None, dict | None]:
    with session_scope() as session:
        return session.execute(
            text("SELECT cluster_id, cluster_terms FROM news WHERE id = :id"), {"id": news_id}
        ).one()


def test_create_news_cluster_links_members_and_is_fetched_as_seed(news_rows):
    first, second = news_rows

    cluster_id = create_news_cluster(
        representative_news_id=first,
        cohesion=0.8,
        term_weights={"삼성전자": 6.0, "유상증자": 3.0},
        keywords=["삼성전자", "유상증자"],
        original_size=3,
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED + timedelta(days=1),
        members=[(first, {"삼성전자": 2.0, "유상증자": 1.0}), (second, {"삼성전자": 2.0})],
    )

    row = _cluster_row(cluster_id)
    assert row["representative_news_id"] == first
    assert row["keywords"] == ["삼성전자", "유상증자"]
    assert row["term_weights"] == {"삼성전자": 6.0, "유상증자": 3.0}
    assert float(row["cohesion"]) == pytest.approx(0.8)
    assert (row["original_size"], row["member_count"]) == (3, 2)
    assert row["first_published_at"] == PUBLISHED
    assert row["last_published_at"] == PUBLISHED + timedelta(days=1)

    assert _news_cluster_columns(first) == (cluster_id, {"삼성전자": 2.0, "유상증자": 1.0})
    assert _news_cluster_columns(second) == (cluster_id, {"삼성전자": 2.0})

    # 윈도우 안이면 시드로 읽힌다. last_stored_date 는 저장 멤버의 최신 보도일(KST)이다.
    [seed] = [
        s
        for s in fetch_active_cluster_seeds(PUBLISHED - timedelta(days=14))
        if s.cluster_id == cluster_id
    ]
    assert seed.term_weights == {"삼성전자": 6.0, "유상증자": 3.0}
    assert (seed.original_size, seed.member_count) == (3, 2)
    assert seed.last_stored_date == (PUBLISHED + timedelta(days=1)).date()
    assert seed.last_published_at == PUBLISHED + timedelta(days=1)

    # 윈도우 밖이면 빠진다.
    assert all(
        s.cluster_id != cluster_id
        for s in fetch_active_cluster_seeds(PUBLISHED + timedelta(days=2))
    )


def test_update_news_cluster_accumulates_and_replaces_representative(news_rows):
    first, second = news_rows
    cluster_id = create_news_cluster(
        representative_news_id=first,
        cohesion=1.0,
        term_weights={"삼성전자": 2.0},
        keywords=["삼성전자"],
        original_size=1,
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED,
        members=[(first, {"삼성전자": 2.0})],
    )

    update_news_cluster(
        cluster_id,
        term_weights={"삼성전자": 4.0, "유상증자": 1.0},
        keywords=["삼성전자", "유상증자"],
        original_size_delta=2,
        first_published_at=PUBLISHED - timedelta(days=1),
        last_published_at=PUBLISHED + timedelta(days=1),
        representative_news_id=second,
        cohesion=0.6,
        members=[(second, {"삼성전자": 2.0, "유상증자": 1.0})],
    )

    row = _cluster_row(cluster_id)
    assert row["representative_news_id"] == second
    assert float(row["cohesion"]) == pytest.approx(0.6)
    assert (row["original_size"], row["member_count"]) == (3, 2)
    assert row["term_weights"] == {"삼성전자": 4.0, "유상증자": 1.0}
    assert row["keywords"] == ["삼성전자", "유상증자"]
    assert row["first_published_at"] == PUBLISHED - timedelta(days=1)  # LEAST
    assert row["last_published_at"] == PUBLISHED + timedelta(days=1)  # GREATEST
    assert _news_cluster_columns(second) == (cluster_id, {"삼성전자": 2.0, "유상증자": 1.0})

    # 저장된 신규 멤버가 없으면 대표·응집도는 그대로, 판정 수와 시간 범위만 는다.
    update_news_cluster(
        cluster_id,
        term_weights={"삼성전자": 6.0, "유상증자": 1.0},
        keywords=["삼성전자", "유상증자"],
        original_size_delta=1,
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED + timedelta(days=5),
        representative_news_id=None,
        cohesion=None,
        members=[],
    )

    row = _cluster_row(cluster_id)
    assert row["representative_news_id"] == second
    assert float(row["cohesion"]) == pytest.approx(0.6)
    assert (row["original_size"], row["member_count"]) == (4, 2)
    assert row["first_published_at"] == PUBLISHED - timedelta(days=1)
    assert row["last_published_at"] == PUBLISHED + timedelta(days=5)

    assert fetch_cluster_member_terms(cluster_id) == [
        (first, {"삼성전자": 2.0}),
        (second, {"삼성전자": 2.0, "유상증자": 1.0}),
    ]
