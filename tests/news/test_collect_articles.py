"""collect_articles job의 순수 로직 단위 테스트 (API/DB 불필요)."""

from __future__ import annotations

from datetime import datetime

from pipelines.news.transformers.clustering import ClusterAssignment, ClusterSeed
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def test_batch_documents_parses_pub_date_and_falls_back():
    from pipelines.news.jobs.collect_articles import batch_documents

    fallback = datetime(2026, 9, 4, 10, tzinfo=SEOUL_TIMEZONE)
    items = [
        {
            "title": "삼성전자 유상증자 결정",
            "description": "삼성전자가 유상증자를 결정했다",
            "pubDate": "Tue, 02 Sep 2026 09:00:00 +0900",
        },
        {"title": "현대차 미국 리콜 확대", "description": "", "pubDate": "not-a-date"},
    ]

    documents, published_ats = batch_documents(
        items, description_weight=0.4, fallback_time=fallback
    )

    assert len(documents) == 2
    assert dict(documents[0])["삼성전자"] > dict(documents[1]).get("삼성전자", 0.0)
    assert published_ats[0] == datetime(2026, 9, 2, 9, tzinfo=SEOUL_TIMEZONE)
    assert published_ats[1] == fallback  # 파싱 실패는 실행 시각으로 본다


def test_cluster_stats_counts_joined_new_and_dropped():
    from pipelines.news.jobs.collect_articles import cluster_stats

    seed = ClusterSeed(
        cluster_id=1,
        term_weights={"a": 1.0},
        original_size=1,
        member_count=1,
        last_stored_date=None,
    )
    at = datetime(2026, 9, 2, tzinfo=SEOUL_TIMEZONE)
    assignments = [
        ClusterAssignment(
            seed=seed,
            members=[0, 1, 2],
            kept=[0],
            dropped=[1, 2],
            term_weights={},
            first_published_at=at,
            last_published_at=at,
            cohesion=None,
        ),
        ClusterAssignment(
            seed=None,
            members=[3],
            kept=[3],
            dropped=[],
            term_weights={},
            first_published_at=at,
            last_published_at=at,
            cohesion=1.0,
        ),
    ]

    stats = cluster_stats(assignments, seed_count=5, collected=4)

    assert stats == {
        "collected": 4,
        "seeds": 5,
        "joined": 1,
        "new_clusters": 1,
        "selected": 2,
        "dropped_by_cluster_cap": 2,
    }
