"""refill_cluster_centroids 통합 테스트 — 멤버 term_weights 합으로 centroids·시각·멤버 수를 다시 유도한다."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_clusters import refill_cluster_centroids
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)


@pytest.fixture
def cluster():
    """V14 직후 옛 클러스터 모양(centroids '{}', member_count 1)과 멤버 3건(그중 1건은 벡터화 전)."""
    marker = uuid.uuid4().hex
    token = f"토큰{marker}"
    with session_scope() as session:
        cluster_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        centroids, member_count, first_published_at, mean_published_at,
                        last_published_at, updated_at
                    )
                    VALUES ('{}', 1, :at, :at, :at, :updated)
                    RETURNING id;
                    """
                ),
                {"at": PUBLISHED, "updated": PUBLISHED},
            ).scalar_one()
        )
        weights = [
            {"title": {token: 3.0, "수주": 1.0}, "body": {"공급": 1.0}, "entities": {"7": 1.0}},
            {"title": {token: 2.0}, "body": {"공급": 0.5, "계약": 1.0}, "entities": {"7": 1.0}},
            None,
        ]
        ids = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO news (title, text, link, published_at, cluster_id, term_weights)
                        VALUES (:title, '본문', :link, :published_at, :cluster_id,
                                CAST(:weights AS jsonb))
                        RETURNING id;
                        """
                    ),
                    {
                        "title": f"centroid 백필 {marker} {i}",
                        "link": f"https://example.com/cc/{marker}/{i}",
                        "published_at": PUBLISHED + timedelta(hours=2 * i),
                        "cluster_id": cluster_id,
                        "weights": json.dumps(w) if w is not None else None,
                    },
                ).scalar_one()
            )
            for i, w in enumerate(weights)
        ]

    yield cluster_id, token

    with session_scope() as session:
        session.execute(text("DELETE FROM news WHERE id = ANY(:ids)"), {"ids": ids})
        session.execute(text("DELETE FROM news_clusters WHERE id = :id"), {"id": cluster_id})


def test_refill_derives_centroids_from_members(cluster):
    cluster_id, token = cluster

    updated = refill_cluster_centroids()

    assert updated >= 1
    with session_scope() as session:
        row = session.execute(
            text(
                "SELECT centroids, member_count, mean_published_at, updated_at "
                "FROM news_clusters WHERE id = :id"
            ),
            {"id": cluster_id},
        ).one()

    assert row.centroids == {
        "title": {token: 5.0, "수주": 1.0},
        "body": {"공급": 1.5, "계약": 1.0},
        "entities": {"7": 2.0},
    }
    assert row.member_count == 3  # 벡터화 전 멤버도 센다
    assert row.mean_published_at == PUBLISHED + timedelta(hours=2)  # (0h + 2h + 4h) / 3
    assert row.updated_at == PUBLISHED  # 건드리지 않는다


def test_refill_is_idempotent(cluster):
    cluster_id, _ = cluster
    refill_cluster_centroids()
    with session_scope() as session:
        first = session.execute(
            text(
                "SELECT centroids, member_count, mean_published_at FROM news_clusters WHERE id = :id"
            ),
            {"id": cluster_id},
        ).one()
    refill_cluster_centroids()
    with session_scope() as session:
        second = session.execute(
            text(
                "SELECT centroids, member_count, mean_published_at FROM news_clusters WHERE id = :id"
            ),
            {"id": cluster_id},
        ).one()
    assert tuple(first) == tuple(second)
