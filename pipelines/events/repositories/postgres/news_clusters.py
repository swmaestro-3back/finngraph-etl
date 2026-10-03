"""승격 후보 클러스터 참조 조회. news_clusters 는 news 도메인이 채우고 여기서는 읽기만 한다."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.models import ClusterCandidate

# EVENT 승격 가능 Cluster 조회
SELECT_PROMOTABLE_SQL = text(
    """
    SELECT
        id,
        representative_news_id,
        keywords,
        original_size,
        member_count,
        first_published_at,
        last_published_at,
        title
    FROM news_clusters
    WHERE original_size >= :min_size
      AND member_count >= 1
      AND updated_at >= :since
    ORDER BY last_published_at DESC;
    """
)


def fetch_promotable_clusters(min_size: int, since: datetime) -> list[ClusterCandidate]:
    """
    Neo4j의 Event 노드로 승격가능한 Cluster 조회
    """
    with session_scope() as session:
        rows = session.execute(
            SELECT_PROMOTABLE_SQL, {"min_size": min_size, "since": since}
        ).fetchall()

    return [
        ClusterCandidate(
            cluster_id=int(row.id),
            representative_news_id=(
                int(row.representative_news_id) if row.representative_news_id is not None else None
            ),
            keywords=list(row.keywords or []),
            original_size=int(row.original_size),
            member_count=int(row.member_count),
            first_published_at=row.first_published_at,
            last_published_at=row.last_published_at,
            title=row.title,
        )
        for row in rows
    ]
