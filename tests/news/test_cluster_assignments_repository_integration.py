"""news_clusters repository 통합 테스트 — 시드 조회, 온라인 판정 기록, df 집계.

실제 Postgres 에 news 행을 만들고 repository 함수를 직접 호출한다. 마이그레이션이 적용된 DB 가
필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_clusters import (
    fetch_cluster_seeds,
    fetch_idf_table,
    record_assignments,
)
from pipelines.news.transformers.clustering.online import ClusterAssignment, ClusterSeed
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

PUBLISHED = datetime(2026, 9, 2, 9, 0, tzinfo=SEOUL_TIMEZONE)


@pytest.fixture
def rows():
    """본문 없는 뉴스 4건과 테스트 전용 토큰. 테스트가 만든 클러스터까지 함께 지운다."""
    marker = uuid.uuid4().hex
    with session_scope() as session:
        ids = [
            int(
                session.execute(
                    text(
                        """
                        INSERT INTO news (title, link, published_at)
                        VALUES (:title, :link, :published_at)
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


def _cluster_row(cluster_id: int) -> dict:
    with session_scope() as session:
        row = session.execute(
            text(
                """
                SELECT representative_news_id, keywords, term_weights, original_size,
                       member_count, first_published_at, last_published_at
                  FROM news_clusters
                 WHERE id = :id;
                """
            ),
            {"id": cluster_id},
        ).one()
    return dict(row._mapping)


def _news_columns(news_id: int) -> tuple[int | None, dict | None]:
    with session_scope() as session:
        row = session.execute(
            text("SELECT cluster_id, cluster_terms FROM news WHERE id = :id"), {"id": news_id}
        ).one()
    return row[0], row[1]


def _new_cluster(token: str, ids: list[int]) -> int:
    """기사 0·1 을 후보로, 2 를 count 만 올리는 기사로 가진 새 클러스터를 기록하고 id 를 돌려준다."""
    documents = [[(token, 2.0), ("공급", 1.0)], [(token, 2.0)], [(token, 2.0)], []]
    assignment = ClusterAssignment(
        seed=None,
        candidates=[0, 1],
        followers=[2],
        term_weights={token: 4.0, "공급": 1.0},
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED + timedelta(hours=2),
    )
    result = record_assignments([assignment], _items(ids), documents, keyword_count=6)
    assert result == {"created": 1, "updated": 0, "failed": 0}
    return _news_columns(ids[0])[0]


def _seed_of(cluster_id: int) -> ClusterSeed:
    [seed] = [
        seed
        for seed in fetch_cluster_seeds(PUBLISHED - timedelta(days=1), PUBLISHED)
        if seed.cluster_id == cluster_id
    ]
    return seed


def test_new_cluster_records_candidates_followers_and_is_read_back_as_seed(rows):
    token, ids = rows

    cluster_id = _new_cluster(token, ids)

    # 후보는 토큰 가중치까지, count 만 올리는 기사는 cluster_id 만 받는다
    assert _news_columns(ids[0]) == (cluster_id, {token: 2.0, "공급": 1.0})
    assert _news_columns(ids[1]) == (cluster_id, {token: 2.0})
    assert _news_columns(ids[2]) == (cluster_id, None)
    assert _news_columns(ids[3]) == (None, None)

    row = _cluster_row(cluster_id)
    assert row["representative_news_id"] is None
    assert (row["member_count"], row["original_size"]) == (2, 3)
    assert row["term_weights"] == {token: 4.0, "공급": 1.0}
    assert row["keywords"] == [token, "공급"]
    assert row["first_published_at"] == PUBLISHED
    assert row["last_published_at"] == PUBLISHED + timedelta(hours=2)

    seed = _seed_of(cluster_id)
    assert seed.promoted is False
    assert seed.member_count == 2
    assert seed.term_weights == {token: 4.0, "공급": 1.0}
    assert seed.first_published_at == PUBLISHED


def test_candidate_joining_seed_updates_profile_and_counters_but_not_anchor(rows):
    token, ids = rows
    cluster_id = _new_cluster(token, ids)
    seed = _seed_of(cluster_id)

    earlier = PUBLISHED - timedelta(hours=5)
    assignment = ClusterAssignment(
        seed=seed,
        candidates=[3],
        followers=[],
        term_weights={token: 6.0, "공급": 1.0},
        first_published_at=seed.first_published_at,
        last_published_at=earlier,
    )
    documents = [[], [], [], [(token, 2.0)]]

    result = record_assignments([assignment], _items(ids), documents, keyword_count=6)

    assert result == {"created": 0, "updated": 1, "failed": 0}
    row = _cluster_row(cluster_id)
    assert (row["member_count"], row["original_size"]) == (3, 4)
    assert row["term_weights"] == {token: 6.0, "공급": 1.0}
    # 기준점은 옮겨지지 않고, 마지막 발행 시각은 더 늦은 값만 받는다
    assert row["first_published_at"] == PUBLISHED
    assert row["last_published_at"] == PUBLISHED + timedelta(hours=2)
    assert _news_columns(ids[3]) == (cluster_id, {token: 2.0})


def test_follower_only_bumps_original_size(rows):
    token, ids = rows
    cluster_id = _new_cluster(token, ids)
    seed = _seed_of(cluster_id)

    later = PUBLISHED + timedelta(days=1)
    assignment = ClusterAssignment(
        seed=seed,
        candidates=[],
        followers=[3],
        term_weights=seed.term_weights,
        first_published_at=seed.first_published_at,
        last_published_at=later,
    )

    result = record_assignments([assignment], _items(ids), [[], [], [], []], keyword_count=6)

    assert result == {"created": 0, "updated": 1, "failed": 0}
    row = _cluster_row(cluster_id)
    assert (row["member_count"], row["original_size"]) == (2, 4)
    assert row["term_weights"] == {token: 4.0, "공급": 1.0}
    assert row["last_published_at"] == later
    assert _news_columns(ids[3]) == (cluster_id, None)


def test_seeds_report_promoted_and_respect_window(rows):
    token, ids = rows
    cluster_id = _new_cluster(token, ids)

    with session_scope() as session:
        session.execute(
            text("UPDATE news_clusters SET representative_news_id = :n WHERE id = :c"),
            {"n": ids[0], "c": cluster_id},
        )

    assert _seed_of(cluster_id).promoted is True
    # 시작이 창보다 이르거나 늦으면 빠진다
    before = fetch_cluster_seeds(PUBLISHED + timedelta(seconds=1), PUBLISHED + timedelta(days=1))
    after = fetch_cluster_seeds(PUBLISHED - timedelta(days=1), PUBLISHED - timedelta(seconds=1))
    assert all(seed.cluster_id != cluster_id for seed in before + after)


def test_idf_table_counts_only_candidate_rows_within_range(rows):
    token, ids = rows
    _new_cluster(token, ids)  # 후보 2건에만 cluster_terms 가 있다

    since = datetime.now(SEOUL_TIMEZONE) - timedelta(hours=1)
    table = fetch_idf_table(since)

    assert table.document_frequency[token] == 2
    assert table.document_count >= 2

    # 집계 범위 밖으로 밀어내면 세지 않는다
    with session_scope() as session:
        session.execute(
            text("UPDATE news SET collected_at = :old WHERE id = ANY(:ids)"),
            {"old": datetime(2020, 1, 1, tzinfo=SEOUL_TIMEZONE), "ids": ids},
        )

    assert token not in fetch_idf_table(since).document_frequency


def test_failed_cluster_is_counted_and_the_rest_are_recorded(rows):
    token, ids = rows
    broken = ClusterAssignment(
        seed=None,
        candidates=[0],
        followers=[],
        term_weights={token: 2.0},
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED,
    )
    good = ClusterAssignment(
        seed=None,
        candidates=[1],
        followers=[],
        term_weights={token: 2.0},
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED,
    )
    # 기사 0 은 저장에 실패해 _news_id 가 없다
    items = [{}, {"_news_id": ids[1]}]

    result = record_assignments([broken, good], items, [[(token, 2.0)]] * 2, keyword_count=6)

    assert result == {"created": 1, "updated": 0, "failed": 1}
    assert _news_columns(ids[0]) == (None, None)
    assert _news_columns(ids[1])[0] is not None


def test_failure_midway_rolls_back_the_whole_cluster(rows):
    token, ids = rows
    assignment = ClusterAssignment(
        seed=None,
        candidates=[0, 1],
        followers=[],
        term_weights={token: 4.0},
        first_published_at=PUBLISHED,
        last_published_at=PUBLISHED,
    )
    # 클러스터 INSERT 와 기사 0 UPDATE 가 성공한 뒤, 기사 1 의 NaN 이 jsonb 변환에서 실패한다
    documents = [[(token, 2.0)], [(token, float("nan"))], [], []]

    result = record_assignments([assignment], _items(ids), documents, keyword_count=6)

    assert result == {"created": 0, "updated": 0, "failed": 1}
    assert _news_columns(ids[0]) == (None, None)
    assert _news_columns(ids[1]) == (None, None)
    with session_scope() as session:
        survivors = session.execute(
            text("SELECT COUNT(*) FROM news_clusters WHERE jsonb_exists(term_weights, :token)"),
            {"token": token},
        ).scalar_one()
    assert survivors == 0
