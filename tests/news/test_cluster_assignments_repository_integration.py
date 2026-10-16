"""news_clusters repository 통합 테스트 — 시드 조회, 판정 기록, 실패 격리.

실제 Postgres(V14 적용)에 news 행을 만들고 repository 함수를 직접 호출한다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_clusters import (
    fetch_cluster_seeds,
    record_assignments,
)
from pipelines.news.transformers.clustering.online import ClusterAssignment, ClusterSeed
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)
FEATURES = {
    "title": 0.9, "body": 0.5, "all": 0.7, "entities": 1.0, "last": 0.99, "mean": 0.98, "first": 0.97,
}  # fmt: skip


@pytest.fixture
def rows():
    """뉴스 4건과 테스트 전용 토큰. 테스트가 만든 클러스터까지 함께 지운다."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        ids = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO news (title, text, link, published_at)
                        VALUES (:title, '본문', :link, :published_at)
                        RETURNING id;
                        """
                    ),
                    {
                        "title": f"판정 기록 {marker} {i}",
                        "link": f"https://example.com/ca/{marker}/{i}",
                        "published_at": PUBLISHED + timedelta(hours=i),
                    },
                ).scalar_one()
            )
            for i in range(4)
        ]

    yield f"토큰{marker}", ids

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


def _items(ids: list[int]) -> list[dict]:
    return [{"_news_id": news_id} for news_id in ids]


def _centroids(token: str, weight: float) -> dict:
    return {"title": {token: weight}, "body": {"공급": 1.0}, "entities": {"7": 1.0}}


def _new_cluster(token: str, ids: list[int], members: list[int]) -> int:
    """members(items 인덱스)로 새 클러스터를 기록하고 cluster_id 를 돌려준다."""
    assignment = ClusterAssignment(
        seed=None,
        members=members,
        scores=[None] + [1.8] * (len(members) - 1),
        features=[None] + [FEATURES] * (len(members) - 1),
        centroids=_centroids(token, 2.0 * len(members)),
        article_count=len(members),
        first_published_at=PUBLISHED,
        mean_published_at=PUBLISHED + timedelta(minutes=30),
        last_published_at=PUBLISHED + timedelta(hours=len(members) - 1),
    )
    assert record_assignments([assignment], _items(ids), 6) == {
        "created": 1,
        "updated": 0,
        "failed": 0,
    }
    with session_scope() as session:
        return int(
            session.execute(
                text("SELECT cluster_id FROM news WHERE id = :id"), {"id": ids[members[0]]}
            ).scalar_one()
        )


def _cluster_row(cluster_id: int) -> dict:
    with session_scope() as session:
        return dict(
            session.execute(
                text(
                    """
                    SELECT representative_news_id, keywords, centroids, member_count,
                           first_published_at, mean_published_at, last_published_at
                      FROM news_clusters WHERE id = :id;
                    """
                ),
                {"id": cluster_id},
            )
            .mappings()
            .one()
        )


def _news_row(news_id: int) -> dict:
    with session_scope() as session:
        return dict(
            session.execute(
                text("SELECT cluster_id, cluster_score, cluster_features FROM news WHERE id = :id"),
                {"id": news_id},
            )
            .mappings()
            .one()
        )


def test_new_cluster_is_recorded_with_centroids_scores_and_features(rows):
    token, ids = rows

    cluster_id = _new_cluster(token, ids, [0, 1])

    row = _cluster_row(cluster_id)
    assert row["representative_news_id"] is None
    assert row["member_count"] == 2
    assert row["centroids"] == _centroids(token, 4.0)
    assert token in row["keywords"]
    assert (row["first_published_at"], row["last_published_at"]) == (
        PUBLISHED,
        PUBLISHED + timedelta(hours=1),
    )
    assert row["mean_published_at"] == PUBLISHED + timedelta(minutes=30)
    first, second = _news_row(ids[0]), _news_row(ids[1])
    assert first == {"cluster_id": cluster_id, "cluster_score": None, "cluster_features": None}
    assert second["cluster_id"] == cluster_id
    assert float(second["cluster_score"]) == pytest.approx(1.8)
    assert second["cluster_features"] == FEATURES


def test_seeds_are_filtered_by_last_and_first_published_at(rows):
    token, ids = rows
    cluster_id = _new_cluster(token, ids, [0, 1])

    seeds = {s.cluster_id: s for s in fetch_cluster_seeds(PUBLISHED, PUBLISHED - timedelta(days=1))}
    assert seeds[cluster_id].article_count == 2
    assert seeds[cluster_id].centroids == _centroids(token, 4.0)
    assert seeds[cluster_id].mean_published_at == PUBLISHED + timedelta(minutes=30)

    # last_published_at(=PUBLISHED+1h) 이 since 보다 이르면 후보가 아니다
    later = PUBLISHED + timedelta(hours=2)
    assert cluster_id not in {s.cluster_id for s in fetch_cluster_seeds(later, PUBLISHED)}
    # first_published_at(=PUBLISHED) 이 first_since 보다 이르면(수명이 다하면) 후보가 아니다
    assert cluster_id not in {
        s.cluster_id for s in fetch_cluster_seeds(PUBLISHED, PUBLISHED + timedelta(minutes=1))
    }


def test_joining_a_seed_updates_centroids_and_time_bounds(rows):
    token, ids = rows
    cluster_id = _new_cluster(token, ids, [0, 1])
    seed = ClusterSeed(
        cluster_id=cluster_id,
        centroids=_centroids(token, 4.0),
        article_count=2,
        first_published_at=PUBLISHED,
        mean_published_at=PUBLISHED + timedelta(minutes=30),
        last_published_at=PUBLISHED + timedelta(hours=1),
    )
    assignment = ClusterAssignment(
        seed=seed,
        members=[2],
        scores=[1.6],
        features=[FEATURES],
        centroids=_centroids(token, 6.0),
        article_count=3,
        first_published_at=PUBLISHED,
        mean_published_at=PUBLISHED + timedelta(hours=1),
        last_published_at=PUBLISHED + timedelta(hours=2),
    )

    assert record_assignments([assignment], _items(ids), 6) == {
        "created": 0,
        "updated": 1,
        "failed": 0,
    }
    row = _cluster_row(cluster_id)
    assert row["member_count"] == 3
    assert row["centroids"] == _centroids(token, 6.0)
    assert row["last_published_at"] == PUBLISHED + timedelta(hours=2)
    assert row["mean_published_at"] == PUBLISHED + timedelta(hours=1)
    assert _news_row(ids[2])["cluster_id"] == cluster_id


def test_record_assignments_isolates_failures(rows):
    """Review Focus 4: 한 클러스터의 기록 실패가 다른 클러스터의 커밋을 막지 않는다.

    실패는 트랜잭션 안에서, 클러스터 INSERT 와 첫 멤버 UPDATE 뒤에 일어나야 한다 — 둘째 멤버의 피처가
    JSON 으로 직렬화되지 않아 터진다. 그래야 INSERT 까지 롤백되는지가 검증된다.
    """
    token, ids = rows
    items = _items(ids)

    def assignment(members, features):
        return ClusterAssignment(
            seed=None,
            members=members,
            scores=[None] + [1.2] * (len(members) - 1),
            features=features,
            centroids=_centroids(token, 2.0),
            article_count=len(members),
            first_published_at=PUBLISHED,
            mean_published_at=PUBLISHED,
            last_published_at=PUBLISHED,
        )

    broken = assignment([2, 3], [None, {"x": object()}])  # object() 는 json.dumps 가 못 쓴다
    healthy = assignment([1], [None])

    result = record_assignments([broken, healthy], items, 6)

    assert result == {"created": 1, "updated": 0, "failed": 1}
    assert _news_row(ids[1])["cluster_id"] is not None
    # 첫 멤버(ids[2])의 UPDATE 는 이미 실행됐지만 같은 트랜잭션이라 함께 롤백된다
    assert _news_row(ids[2])["cluster_id"] is None
    assert _news_row(ids[3])["cluster_id"] is None  # 다음 런에 다시 판정된다
    with session_scope() as session:
        count = session.execute(
            text("SELECT COUNT(*) FROM news_clusters WHERE jsonb_exists(centroids -> 'title', :t)"),
            {"t": token},
        ).scalar_one()
    assert count == 1  # 실패한 클러스터는 INSERT 도 롤백된다


def test_seed_from_legacy_row_has_empty_centroid_fields(rows):
    """Review Focus 5: V14 직후 옛 행(centroids '{}')도 필드 세 개가 빈 dict 인 시드가 된다."""
    _, ids = rows
    with session_scope() as session:
        cluster_id = int(
            session.execute(
                text(
                    """
                    INSERT INTO news_clusters (member_count, first_published_at, mean_published_at,
                                               last_published_at)
                    VALUES (3, :p, :p, :p) RETURNING id;
                    """
                ),
                {"p": PUBLISHED},
            ).scalar_one()
        )
        session.execute(
            text("UPDATE news SET cluster_id = :c WHERE id = :n"), {"c": cluster_id, "n": ids[0]}
        )

    seeds = {s.cluster_id: s for s in fetch_cluster_seeds(PUBLISHED, PUBLISHED)}

    assert seeds[cluster_id].centroids == {"title": {}, "body": {}, "entities": {}}
    assert seeds[cluster_id].article_count == 3
