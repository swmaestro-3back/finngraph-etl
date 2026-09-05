"""RDB 참조 조회 — 승격 후보 클러스터와 그 멤버.

읽기 전용이라 references/ 에 둔다(쓰기는 loaders/). 원천은 news_clusters / news 이고 이
파이프라인은 RDB 에 아무것도 쓰지 않는다.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.models import ClusterCandidate, MemberArticle

# 윈도우를 벗어난 클러스터는 더 이상 바뀌지 않으니 updated_at 으로 스캔 범위를 자른다.
# LIMIT 은 없다 — 갱신은 LLM 이 없어 싸고, 상한은 job 이 생성에만 건다.
SELECT_PROMOTABLE_SQL = text(
    """
    SELECT id, representative_news_id, keywords, original_size, member_count,
           first_published_at, last_published_at
    FROM news_clusters
    WHERE original_size >= :min_size
      AND member_count >= 1
      AND updated_at >= :since
    ORDER BY last_published_at DESC;
    """
)

SELECT_MEMBERS_SQL = text(
    """
    SELECT id, cluster_id, title, summary, text, published_at
    FROM news
    WHERE cluster_id = ANY(:cluster_ids)
    ORDER BY cluster_id, published_at ASC NULLS LAST, id ASC;
    """
)

SELECT_NEWS_IDS_SQL = text(
    """
    SELECT cluster_id, id
    FROM news
    WHERE cluster_id = ANY(:cluster_ids)
    ORDER BY cluster_id, id ASC;
    """
)


def fetch_promotable_clusters(min_size: int, since: datetime) -> list[ClusterCandidate]:
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
        )
        for row in rows
    ]


def fetch_cluster_members(cluster_ids: list[int]) -> dict[int, list[MemberArticle]]:
    """저장 멤버 전체. 순서는 published_at 오름차순(NULL 은 뒤), 같으면 id 오름차순."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(SELECT_MEMBERS_SQL, {"cluster_ids": cluster_ids}).fetchall()

    members: dict[int, list[MemberArticle]] = defaultdict(list)
    for row in rows:
        members[int(row.cluster_id)].append(
            MemberArticle(
                news_id=int(row.id),
                title=row.title or "",
                summary=row.summary,
                text=row.text or "",
                published_at=row.published_at,
            )
        )
    return dict(members)


def fetch_cluster_news_ids(cluster_ids: list[int]) -> dict[int, list[int]]:
    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(SELECT_NEWS_IDS_SQL, {"cluster_ids": cluster_ids}).fetchall()

    news_ids: dict[int, list[int]] = defaultdict(list)
    for cluster_id, news_id in rows:
        news_ids[int(cluster_id)].append(int(news_id))
    return dict(news_ids)
