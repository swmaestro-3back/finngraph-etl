"""이슈 타임라인 연결용 news_clusters 읽기·쓰기.

판정 규칙은 transformers/issue_linker.py, 흐름은 jobs/link_issues.py. 벡터는 pgvector 텍스트
표현('[x,y,...]')으로 주고받고 SQL 에서 CAST 한다 — pgvector 파이썬 패키지를 쓰지 않는다.

연결 쓰기는 updated_at 을 건드리지 않는다. events 승격 스캔이 updated_at 창으로 후보를 고르므로,
연결이 옛 클러스터를 다시 깨우면 안 된다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.issue_linker import LinkCandidate

# 이름이 붙었고 아직 연결 판정 전인 클러스터. 오래된 것부터 — 앞선 대상이 뒤 대상의 후보가 된다
SELECT_LINK_TARGETS_SQL = text(
    """
    SELECT id, title, summary, first_published_at, embedding::text AS embedding
      FROM news_clusters
     WHERE title IS NOT NULL
       AND linked_at IS NULL
       AND first_published_at >= :since
     ORDER BY first_published_at ASC, id ASC
     LIMIT :limit;
    """
)

COUNT_LINK_TARGETS_SQL = text(
    """
    SELECT COUNT(*) AS targets,
           COUNT(*) FILTER (WHERE embedding IS NULL) AS without_embedding
      FROM news_clusters
     WHERE title IS NOT NULL
       AND linked_at IS NULL
       AND first_published_at >= :since;
    """
)

# 클러스터별 최신 멤버 기사 제목 (임베딩 텍스트용)
SELECT_RECENT_MEMBER_TITLES_SQL = text(
    """
    SELECT cluster_id, title
      FROM (
        SELECT cluster_id,
               title,
               ROW_NUMBER() OVER (
                 PARTITION BY cluster_id
                 ORDER BY published_at DESC NULLS LAST, id DESC
               ) AS rn
          FROM news
         WHERE cluster_id = ANY(:cluster_ids)
           AND title IS NOT NULL
           AND BTRIM(title) <> ''
      ) ranked
     WHERE rn <= :per_cluster
     ORDER BY cluster_id, rn;
    """
)

# 클러스터의 기업 = 멤버 기사에 연결된 company_id 의 합집합
SELECT_CLUSTER_COMPANY_IDS_SQL = text(
    """
    SELECT n.cluster_id, ARRAY_AGG(DISTINCT x.company_id) AS company_ids
      FROM news n
      JOIN news_companies x ON x.news_id = n.id
     WHERE n.cluster_id = ANY(:cluster_ids)
     GROUP BY n.cluster_id;
    """
)

UPDATE_CLUSTER_EMBEDDING_SQL = text(
    """
    UPDATE news_clusters
       SET embedding = CAST(:embedding AS vector)
     WHERE id = :cluster_id;
    """
)

# 부모 후보: 대상보다 먼저 시작했고 lookback 안이며 이미 연결 판정이 끝난(story_root_id 가 있는)
# 클러스터. {company_filter} 는 기업 겹침(대상에 기업이 있을 때) 또는 기업 없음 조건이다.
# 점수 순서와 동점 처리는 issue_linker.choose_parent 와 같다.
SELECT_LINK_CANDIDATES_SQL = """
    SELECT s.id,
           s.first_published_at,
           s.score,
           ARRAY(
             SELECT DISTINCT x.company_id
               FROM news n
               JOIN news_companies x ON x.news_id = n.id
              WHERE n.cluster_id = s.id
           ) AS company_ids
      FROM (
        SELECT c.id,
               c.first_published_at,
               1 - (c.embedding <=> CAST(:embedding AS vector)) AS score
          FROM news_clusters c
         WHERE c.id <> :cluster_id
           AND c.linked_at IS NOT NULL
           AND c.embedding IS NOT NULL
           AND c.first_published_at < :first_published_at
           AND c.first_published_at >= :window_start
           AND {company_filter}
      ) s
     WHERE s.score >= :min_score
     ORDER BY s.score DESC, s.first_published_at DESC, s.id DESC
     LIMIT :limit;
"""

SHARES_COMPANY_FILTER = """EXISTS (
               SELECT 1
                 FROM news n
                 JOIN news_companies x ON x.news_id = n.id
                WHERE n.cluster_id = c.id
                  AND x.company_id = ANY(:company_ids)
           )"""

NO_COMPANY_FILTER = """NOT EXISTS (
               SELECT 1
                 FROM news n
                 JOIN news_companies x ON x.news_id = n.id
                WHERE n.cluster_id = c.id
           )"""

# 부모가 있으면 부모의 루트를 물려받고, 없으면 자기 자신이 루트다. linked_at IS NULL 조건으로
# 같은 클러스터를 두 번 판정하지 않는다.
UPDATE_LINK_DECISION_SQL = text(
    """
    UPDATE news_clusters
       SET parent_cluster_id = CAST(:parent_id AS BIGINT),
           story_root_id = COALESCE(
             (
               SELECT COALESCE(p.story_root_id, p.id)
                 FROM news_clusters p
                WHERE p.id = CAST(:parent_id AS BIGINT)
             ),
             id
           ),
           link_score = CAST(:link_score AS NUMERIC),
           linked_at = now()
     WHERE id = :cluster_id
       AND linked_at IS NULL;
    """
)


# 잘못된 순서로 돈 연결을 되돌린다(백필 --reset-links). 임베딩까지 지워 요약을 넣어 다시 만든다.
# 부모는 늘 더 먼저 시작한 클러스터라, first_published_at 이 since 이후인 구간을 통째로 지우면
# 그 앞의 판정은 지워진 클러스터를 가리키지 않는다.
COUNT_RESETTABLE_SQL = text(
    """
    SELECT COUNT(*) AS clusters,
           COUNT(*) FILTER (WHERE linked_at IS NOT NULL) AS linked
      FROM news_clusters
     WHERE first_published_at >= :since
       AND (linked_at IS NOT NULL OR embedding IS NOT NULL);
    """
)

RESET_LINKS_SQL = text(
    """
    UPDATE news_clusters
       SET embedding = NULL,
           parent_cluster_id = NULL,
           story_root_id = NULL,
           link_score = NULL,
           linked_at = NULL
     WHERE first_published_at >= :since
       AND (linked_at IS NOT NULL OR embedding IS NOT NULL);
    """
)


@dataclass(frozen=True)
class LinkTarget:
    """연결 판정 대상 클러스터. embedding 은 pgvector 텍스트 표현이고 아직 없으면 None."""

    cluster_id: int
    title: str
    summary: str | None
    first_published_at: datetime
    embedding: str | None = None


def fetch_link_targets(since: datetime, limit: int) -> list[LinkTarget]:
    """이름이 있고 linked_at 이 NULL 이며 first_published_at 이 since 이후인 클러스터, 오래된 순"""

    with session_scope() as session:
        rows = session.execute(SELECT_LINK_TARGETS_SQL, {"since": since, "limit": limit}).fetchall()

    return [
        LinkTarget(
            cluster_id=int(row.id),
            title=row.title,
            summary=row.summary,
            first_published_at=row.first_published_at,
            embedding=row.embedding,
        )
        for row in rows
    ]


def count_link_targets(since: datetime) -> tuple[int, int]:
    """(연결 대상 수, 그중 임베딩이 없는 수)"""

    with session_scope() as session:
        row = session.execute(COUNT_LINK_TARGETS_SQL, {"since": since}).one()

    return int(row.targets), int(row.without_embedding)


def fetch_recent_member_titles(cluster_ids: list[int], per_cluster: int) -> dict[int, list[str]]:
    """클러스터별 멤버 기사 제목, 최신순 per_cluster 개"""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_RECENT_MEMBER_TITLES_SQL,
            {"cluster_ids": [int(c) for c in cluster_ids], "per_cluster": per_cluster},
        ).fetchall()

    titles: dict[int, list[str]] = {}
    for cluster_id, title in rows:
        titles.setdefault(int(cluster_id), []).append(title)

    return titles


def fetch_cluster_company_ids(cluster_ids: list[int]) -> dict[int, frozenset[int]]:
    """클러스터별 기업 집합. 연결된 기업이 없는 클러스터는 결과에 없다."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_COMPANY_IDS_SQL, {"cluster_ids": [int(c) for c in cluster_ids]}
        ).fetchall()

    return {
        int(cluster_id): frozenset(int(c) for c in company_ids or [])
        for cluster_id, company_ids in rows
    }


def save_cluster_embedding(cluster_id: int, embedding: str) -> None:
    with session_scope() as session:
        session.execute(
            UPDATE_CLUSTER_EMBEDDING_SQL, {"cluster_id": cluster_id, "embedding": embedding}
        )


def fetch_link_candidates(
    *,
    cluster_id: int,
    embedding: str,
    first_published_at: datetime,
    company_ids: frozenset[int],
    lookback_days: int,
    min_score: float,
    limit: int,
) -> list[LinkCandidate]:
    """대상의 부모 후보를 코사인 내림차순으로. 기업 규칙과 min_score 는 SQL 에서 먼저 거른다."""

    company_filter = SHARES_COMPANY_FILTER if company_ids else NO_COMPANY_FILTER
    params = {
        "cluster_id": cluster_id,
        "embedding": embedding,
        "first_published_at": first_published_at,
        "window_start": first_published_at - timedelta(days=lookback_days),
        "min_score": min_score,
        "limit": limit,
    }
    if company_ids:
        params["company_ids"] = sorted(company_ids)

    with session_scope() as session:
        rows = session.execute(
            text(SELECT_LINK_CANDIDATES_SQL.format(company_filter=company_filter)), params
        ).fetchall()

    return [
        LinkCandidate(
            cluster_id=int(row.id),
            first_published_at=row.first_published_at,
            score=float(row.score),
            company_ids=frozenset(int(c) for c in row.company_ids or []),
        )
        for row in rows
    ]


def record_link_decision(cluster_id: int, parent_id: int | None, score: float | None) -> bool:
    """연결 판정을 쓴다. 부모가 없으면 루트. 이미 판정된 클러스터면 False."""

    with session_scope() as session:
        updated = session.execute(
            UPDATE_LINK_DECISION_SQL,
            {
                "cluster_id": cluster_id,
                "parent_id": parent_id,
                # vector 가 float4 라 코사인 끝자리는 잡음이다
                "link_score": None if score is None else round(score, 6),
            },
        ).rowcount

    return updated > 0


def count_resettable_clusters(since: datetime) -> tuple[int, int]:
    """(임베딩이나 연결 판정이 있는 클러스터 수, 그중 판정된 수). first_published_at >= since"""

    with session_scope() as session:
        row = session.execute(COUNT_RESETTABLE_SQL, {"since": since}).one()

    return int(row.clusters), int(row.linked)


def reset_links(since: datetime) -> int:
    """since 이후 시작한 클러스터의 임베딩과 연결 판정을 지운다. 지운 클러스터 수."""

    with session_scope() as session:
        reset = session.execute(RESET_LINKS_SQL, {"since": since}).rowcount

    return reset
