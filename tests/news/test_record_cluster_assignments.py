"""record_cluster_assignments 단위 테스트. DB 쓰기 함수를 갈아끼우고 클러스터 id 반환만 본다."""

from __future__ import annotations

from datetime import date, datetime

from pipelines.news.repositories import news_clusters
from pipelines.news.transformers.clustering import ClusterAssignment, ClusterSeed
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

AT = datetime(2026, 9, 2, 9, tzinfo=SEOUL_TIMEZONE)


def _assignment(members: list[int], seed: ClusterSeed | None = None) -> ClusterAssignment:
    return ClusterAssignment(
        seed=seed,
        members=members,
        kept=members,
        dropped=[],
        term_weights={"삼성전자": 1.0},
        first_published_at=AT,
        last_published_at=AT,
        cohesion=None if seed else 1.0,
    )


def _seed(cluster_id: int) -> ClusterSeed:
    return ClusterSeed(
        cluster_id=cluster_id,
        term_weights={"삼성전자": 1.0},
        original_size=1,
        member_count=1,
        last_stored_date=date(2026, 9, 1),
        first_published_at=AT,
    )


def test_returns_cluster_id_per_assignment_in_order(monkeypatch):
    created_members: list[list[int]] = []

    def fake_create(**kwargs):
        created_members.append([news_id for news_id, _ in kwargs["members"]])
        return 500 + len(created_members)

    def fake_seed_update(assignment, members, survivor_terms, keyword_count):
        if assignment.seed.cluster_id == 8:
            raise RuntimeError("deadlock")

    monkeypatch.setattr(news_clusters, "create_news_cluster", fake_create)
    monkeypatch.setattr(news_clusters, "_record_seed_update", fake_seed_update)

    items = [
        {"_news_id": 900, "_save_action": "inserted"},  # 새 클러스터 → 생성
        {},  # 새 클러스터인데 저장된 기사 없음 → 만들지 않음
        {"_news_id": 901, "_save_action": "inserted"},  # 시드 7 합류 → 갱신
        {"_news_id": 902, "_save_action": "inserted"},  # 시드 8 합류 → 기록 실패
    ]
    assignments = [
        _assignment([0]),
        _assignment([1]),
        _assignment([2], seed=_seed(7)),
        _assignment([3], seed=_seed(8)),
    ]
    documents = [[("삼성전자", 1.0)]] * len(items)

    counts, cluster_ids = news_clusters.record_cluster_assignments(
        assignments, items, documents, keyword_count=5
    )

    assert counts == {"created": 1, "updated": 1, "failed": 1}
    assert cluster_ids == [501, None, 7, None]
    assert created_members == [[900]]
