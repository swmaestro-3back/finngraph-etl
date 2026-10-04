"""Event 로 올릴 클러스터 조회. news_clusters 는 news 도메인이 채우고 여기서는 읽기만 한다."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.models import ClusterCandidate

# 대표와 제목이 있는 클러스터. member_count 조건은 옛 로직이 만든 작은 클러스터(대표는 있지만
# 후보 수가 승격 기준 미만)를 뺀다.
SELECT_PROMOTED_SQL = text(
    """
    SELECT id, title, first_published_at
      FROM news_clusters
     WHERE representative_news_id IS NOT NULL
       AND title IS NOT NULL
       AND member_count >= :promote_size
       AND updated_at >= :since
     ORDER BY first_published_at DESC, id DESC;
    """
)


def fetch_promoted_clusters(promote_size: int, since: datetime) -> list[ClusterCandidate]:
    """since 이후 갱신된 클러스터 중 승격되고 제목이 붙은 것 (최신 사건 먼저)"""

    with session_scope() as session:
        rows = session.execute(
            SELECT_PROMOTED_SQL, {"promote_size": promote_size, "since": since}
        ).fetchall()

    return [
        ClusterCandidate(
            cluster_id=int(row.id), title=row.title, first_published_at=row.first_published_at
        )
        for row in rows
    ]
