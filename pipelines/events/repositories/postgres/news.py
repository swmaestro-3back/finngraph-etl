"""클러스터 소속 뉴스 참조 조회. news 는 news 도메인이 채우고 여기서는 읽기만 한다."""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.events.models import MemberArticle

# 특정 Cluster에 속한 News 조회
SELECT_MEMBERS_SQL = text(
    """
    SELECT
        id,
        cluster_id,
        title,
        summary,
        text,
        published_at
    FROM news
    WHERE cluster_id = ANY(:cluster_ids)
    ORDER BY cluster_id, published_at ASC NULLS LAST, id ASC;
    """
)

SELECT_NEWS_IDS_SQL = text(
    """
    SELECT
        cluster_id,
        id
    FROM news
    WHERE cluster_id = ANY(:cluster_ids)
    ORDER BY cluster_id, id ASC;
    """
)


def fetch_cluster_members(cluster_ids: list[int]) -> dict[int, list[MemberArticle]]:
    """
    새 EVENT 노드 생성을 위한 Cluster 정보 조회
    LLM에게 제목, 요약, 날짜를 전달해주어야 하므로 해당 칼럼값들을 조회한다
    """

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
    """
    기존에 존재하는 EVENT 노드 갱신용
    EVENT로 존재하는 Cluster는 LLM을 통해 제목을 다시 만들지 않으므로
    본문이 필요없고 노드의 news_ids를 업데이트하기 위해 news_id 목록만 조회하면 된다.
    """

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(SELECT_NEWS_IDS_SQL, {"cluster_ids": cluster_ids}).fetchall()

    news_ids: dict[int, list[int]] = defaultdict(list)
    for cluster_id, news_id in rows:
        news_ids[int(cluster_id)].append(int(news_id))
    return dict(news_ids)
