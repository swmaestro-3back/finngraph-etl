"""news_cluster_judgments 쓰기 — 배치 클러스터 판정의 기사 단위 기록.

행은 transformers/clustering/judgments.py 가 만든다. 평가용 기록이라 실패해도 수집을 멈추지
않도록 호출부(jobs/collect_articles.py)가 예외를 삼킨다.
"""

from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.clustering.judgments import ClusterJudgment

INSERT_CLUSTER_JUDGMENT_SQL = text(
    """
    INSERT INTO news_cluster_judgments (
        run_at, link, title, description, published_at,
        cluster_id, is_new_cluster, kept, seed_similarity
    )
    VALUES (
        :run_at, :link, :title, :description, :published_at,
        :cluster_id, :is_new_cluster, :kept, :seed_similarity
    );
    """
)


def insert_cluster_judgments(judgments: list[ClusterJudgment]) -> int:
    """판정 행을 한 트랜잭션에 넣는다(executemany). 넣은 행 수를 돌려준다."""

    if not judgments:
        return 0

    with session_scope() as session:
        session.execute(INSERT_CLUSTER_JUDGMENT_SQL, [asdict(j) for j in judgments])

    return len(judgments)
