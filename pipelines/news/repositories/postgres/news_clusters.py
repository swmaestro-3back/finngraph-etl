import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.clustering.online import (
    ClusterAssignment,
    ClusterSeed,
    Terms,
    sum_terms,
    top_keywords,
)
from pipelines.news.transformers.clustering.vectorize import IdfTable

# 멤버 기사에 클러스터와 토큰 가중치 기록
UPDATE_NEWS_CLUSTER_MEMBER_SQL = text(
    """
    UPDATE news
       SET cluster_id = :cluster_id,
           cluster_terms = CAST(:terms AS jsonb)
     WHERE id = :news_id;
    """
)


UPDATE_CLUSTER_TITLE_SQL = text(
    """
    UPDATE news_clusters
       SET title = :title,
           updated_at = now()
     WHERE id = :cluster_id;
    """
)


def update_cluster_title(cluster_id: int, title: str) -> None:
    with session_scope() as session:
        session.execute(UPDATE_CLUSTER_TITLE_SQL, {"cluster_id": cluster_id, "title": title})


# ── 온라인 판정 ──────────────────────────────────────────────────────────────

# 후보 기사(cluster_terms 가 있는 행)의 토큰별 문서 수. 승격 후 기사는 cluster_terms 가 NULL 이라
# 들지 않는다 — 큰 사건이 df 를 부풀리지 못한다.
SELECT_TERM_DOCUMENT_FREQUENCY_SQL = text(
    """
    SELECT term.token, COUNT(*) AS df
      FROM news n
     CROSS JOIN LATERAL jsonb_object_keys(n.cluster_terms) AS term(token)
     WHERE n.cluster_terms IS NOT NULL
       AND jsonb_typeof(n.cluster_terms) = 'object'
       AND n.collected_at >= :since
     GROUP BY term.token;
    """
)

SELECT_TERM_DOCUMENT_COUNT_SQL = text(
    """
    SELECT COUNT(*)
      FROM news n
     WHERE n.cluster_terms IS NOT NULL
       AND jsonb_typeof(n.cluster_terms) = 'object'
       AND n.collected_at >= :since;
    """
)

# first_published_at 이 [start, end] 안인 클러스터와 그 프로필
SELECT_CLUSTER_SEEDS_SQL = text(
    """
    SELECT id,
           term_weights,
           member_count,
           representative_news_id IS NOT NULL AS promoted,
           first_published_at
      FROM news_clusters
     WHERE first_published_at BETWEEN :window_start AND :window_end
     ORDER BY id ASC;
    """
)

# 새 클러스터. 대표는 승격 때 정하므로 비워 둔다.
INSERT_COLLECTING_CLUSTER_SQL = text(
    """
    INSERT INTO news_clusters (
        keywords, term_weights, original_size, member_count,
        first_published_at, last_published_at
    )
    VALUES (
        CAST(:keywords AS text[]), CAST(:term_weights AS jsonb), :original_size, :member_count,
        :first_published_at, :last_published_at
    )
    RETURNING id;
    """
)

# 후보가 편입된 클러스터: 프로필을 바꾸고 카운터를 올린다. first_published_at 은 옮기지 않는다.
UPDATE_CLUSTER_WITH_CANDIDATES_SQL = text(
    """
    UPDATE news_clusters
       SET term_weights = CAST(:term_weights AS jsonb),
           keywords = CAST(:keywords AS text[]),
           original_size = original_size + :size_delta,
           member_count = member_count + :member_delta,
           last_published_at = GREATEST(last_published_at, :last_published_at),
           updated_at = now()
     WHERE id = :cluster_id;
    """
)

# count 만 올리는 클러스터: 프로필과 member_count 는 그대로다.
UPDATE_CLUSTER_COUNT_ONLY_SQL = text(
    """
    UPDATE news_clusters
       SET original_size = original_size + :size_delta,
           last_published_at = GREATEST(last_published_at, :last_published_at),
           updated_at = now()
     WHERE id = :cluster_id;
    """
)

UPDATE_NEWS_FOLLOWER_SQL = text(
    """
    UPDATE news
       SET cluster_id = :cluster_id
     WHERE id = :news_id;
    """
)


def fetch_idf_table(since: datetime) -> IdfTable:
    """since 이후 수집된 후보 기사의 토큰별 문서 수. 런 시작 때 한 번 읽는다."""

    with session_scope() as session:
        rows = session.execute(SELECT_TERM_DOCUMENT_FREQUENCY_SQL, {"since": since}).fetchall()
        count = session.execute(SELECT_TERM_DOCUMENT_COUNT_SQL, {"since": since}).scalar_one()

    return IdfTable(
        document_frequency={token: int(df) for token, df in rows},
        document_count=int(count),
    )


def fetch_cluster_seeds(window_start: datetime, window_end: datetime) -> list[ClusterSeed]:
    """first_published_at 이 [window_start, window_end] 안인 클러스터의 판정용 스냅샷."""

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_SEEDS_SQL, {"window_start": window_start, "window_end": window_end}
        ).fetchall()

        return [
            ClusterSeed(
                cluster_id=int(row.id),
                term_weights={token: float(weight) for token, weight in row.term_weights.items()},
                member_count=int(row.member_count),
                promoted=bool(row.promoted),
                first_published_at=row.first_published_at,
            )
            for row in rows
        ]


def record_assignments(
    assignments: list[ClusterAssignment],
    items: list[dict[str, Any]],
    documents: list[Terms],
    keyword_count: int,
) -> dict[str, int]:
    """온라인 판정 결과를 news_clusters 와 news.cluster_id 에 기록한다.

    후보 기사는 cluster_id 와 cluster_terms 를, count 만 올리는 기사는 cluster_id 만 받는다.
    클러스터 하나가 트랜잭션 하나다. 실패한 클러스터는 건너뛰고 세어 돌려주며, 그 기사들은
    news 에 cluster_id 없이 남는다(news 저장은 이미 커밋됐다). 다음 cluster 런이 다시 판정한다.
    """

    created = 0
    updated = 0
    failed = 0

    for assignment in assignments:
        try:
            candidates = [
                (int(items[index]["_news_id"]), sum_terms(documents[index]))
                for index in assignment.candidates
            ]
            followers = [int(items[index]["_news_id"]) for index in assignment.followers]
            size = len(candidates) + len(followers)

            with session_scope() as session:
                if assignment.seed is None:
                    cluster_id = int(
                        session.execute(
                            INSERT_COLLECTING_CLUSTER_SQL,
                            {
                                "keywords": top_keywords(assignment.term_weights, keyword_count),
                                "term_weights": json.dumps(
                                    assignment.term_weights, ensure_ascii=False
                                ),
                                "original_size": size,
                                "member_count": len(candidates),
                                "first_published_at": assignment.first_published_at,
                                "last_published_at": assignment.last_published_at,
                            },
                        ).scalar_one()
                    )
                else:
                    cluster_id = assignment.seed.cluster_id
                    if candidates:
                        session.execute(
                            UPDATE_CLUSTER_WITH_CANDIDATES_SQL,
                            {
                                "cluster_id": cluster_id,
                                "term_weights": json.dumps(
                                    assignment.term_weights, ensure_ascii=False
                                ),
                                "keywords": top_keywords(assignment.term_weights, keyword_count),
                                "size_delta": size,
                                "member_delta": len(candidates),
                                "last_published_at": assignment.last_published_at,
                            },
                        )
                    else:
                        session.execute(
                            UPDATE_CLUSTER_COUNT_ONLY_SQL,
                            {
                                "cluster_id": cluster_id,
                                "size_delta": size,
                                "last_published_at": assignment.last_published_at,
                            },
                        )

                for news_id, terms in candidates:
                    session.execute(
                        UPDATE_NEWS_CLUSTER_MEMBER_SQL,
                        {
                            "cluster_id": cluster_id,
                            "terms": json.dumps(terms, ensure_ascii=False),
                            "news_id": news_id,
                        },
                    )
                for news_id in followers:
                    session.execute(
                        UPDATE_NEWS_FOLLOWER_SQL, {"cluster_id": cluster_id, "news_id": news_id}
                    )
        except Exception as e:
            failed += 1
            # 일부 기사는 _news_id 가 없을 수 있다(그것이 실패 원인일 수 있다)
            news_ids = [
                items[index].get("_news_id")
                for index in assignment.candidates + assignment.followers
            ]
            logging.error(
                "클러스터 기록 실패(건너뜀): "
                f"cluster_id={assignment.seed.cluster_id if assignment.seed else None}, "
                f"news_ids={news_ids}, error={type(e).__name__}: {e}"
            )
            continue

        if assignment.seed is None:
            created += 1
        else:
            updated += 1

    return {"created": created, "updated": updated, "failed": failed}


# 아직 클러스터 판정을 받지 않은 저장 기사. 수집 잡이 본문까지 채워 저장하므로 본문이 없는 행은
# 옛 구조에서 남은 것이고 판정하지 않는다. 발행 시각이 없으면 수집 시각으로 본다.
SELECT_UNCLUSTERED_NEWS_SQL = text(
    """
    SELECT id, title, text, COALESCE(published_at, collected_at, now()) AS published_at
      FROM news
     WHERE cluster_id IS NULL
       AND text IS NOT NULL
       AND BTRIM(text) <> ''
     ORDER BY 4 ASC, id ASC;
    """
)


def fetch_unclustered_news() -> list[dict[str, Any]]:
    """클러스터 판정 대상 전량 (발행 시각순). 기록에 실패해 cluster_id 가 없는 기사도 다시 든다."""

    with session_scope() as session:
        rows = session.execute(SELECT_UNCLUSTERED_NEWS_SQL).fetchall()

    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "text": body or "",
            "published_at": published_at,
        }
        for news_id, title, body, published_at in rows
    ]


# SELECT_UNCLUSTERED_NEWS_SQL 과 같은 조건이다.
COUNT_UNCLUSTERED_NEWS_SQL = text(
    """
    SELECT COUNT(*)
      FROM news
     WHERE cluster_id IS NULL
       AND text IS NOT NULL
       AND BTRIM(text) <> '';
    """
)


def count_unclustered_news() -> int:
    """클러스터 판정 대상 기사 수. 백필 DAG 가 하류를 깨울지 정할 때 쓴다."""

    with session_scope() as session:
        return int(session.execute(COUNT_UNCLUSTERED_NEWS_SQL).scalar_one())


# ── 승격 ─────────────────────────────────────────────────────────────────────

# 후보가 다 찼는데 대표가 없는 클러스터. since 는 재시도 범위다 — 오래 갱신되지 않은 클러스터는
# 다시 시도하지 않는다.
SELECT_PROMOTABLE_CLUSTER_IDS_SQL = text(
    """
    SELECT id
      FROM news_clusters
     WHERE representative_news_id IS NULL
       AND member_count >= :promote_size
       AND updated_at >= :since
     ORDER BY id;
    """
)

SET_CLUSTER_REPRESENTATIVE_SQL = text(
    """
    UPDATE news_clusters
       SET representative_news_id = :news_id,
           updated_at = now()
     WHERE id = :cluster_id
       AND representative_news_id IS NULL;
    """
)

# 후보 기사(cluster_terms 가 있는 행)와 저장된 본문. 발행 시각순
SELECT_CLUSTER_CANDIDATES_SQL = text(
    """
    SELECT cluster_id, id, title, text, published_at
      FROM news
     WHERE cluster_id = ANY(:cluster_ids)
       AND cluster_terms IS NOT NULL
     ORDER BY cluster_id, published_at ASC NULLS LAST, id ASC;
    """
)

SELECT_CLUSTER_MEMBER_SQL = text(
    """
    SELECT 1
      FROM news
     WHERE id = :news_id
       AND cluster_id = :cluster_id;
    """
)

# 승격됐는데 제목이 없는 클러스터. member_count 조건은 옛 로직이 만든 작은 클러스터(대표는
# 있지만 후보 수가 기준 미만)를 뺀다. 기간 제한이 없어 실패한 클러스터는 언제든 다시 시도된다.
SELECT_UNTITLED_PROMOTED_SQL = text(
    """
    SELECT id
      FROM news_clusters
     WHERE representative_news_id IS NOT NULL
       AND title IS NULL
       AND member_count >= :promote_size
     ORDER BY id;
    """
)

# 삼중항 추출을 아직 시도하지 않은 대표 기사 수. 0 이 아니면 cluster DAG 가 삼중항 DAG 를 깨운다.
# member_count 조건은 옛 로직이 만든 작은 클러스터의 대표를 뺀다(삼중항 대상 조회와 같은 기준).
SELECT_PENDING_REPRESENTATIVES_SQL = text(
    """
    SELECT COUNT(*)
      FROM news n
      JOIN news_clusters nc ON nc.representative_news_id = n.id
     WHERE n.triple_extracted IS NULL
       AND nc.member_count >= :promote_size;
    """
)


def fetch_promotable_cluster_ids(promote_size: int, since: datetime) -> list[int]:
    """후보가 promote_size 건 이상이고 대표가 없는, since 이후 갱신된 클러스터"""

    with session_scope() as session:
        rows = session.execute(
            SELECT_PROMOTABLE_CLUSTER_IDS_SQL, {"promote_size": promote_size, "since": since}
        ).fetchall()

    return [int(row[0]) for row in rows]


def fetch_cluster_candidates(cluster_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """클러스터별 후보 기사 (발행 시각순). 대표 선정과 제목 생성의 입력이다."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_CANDIDATES_SQL, {"cluster_ids": [int(c) for c in cluster_ids]}
        ).fetchall()

    candidates: dict[int, list[dict[str, Any]]] = {}
    for cluster_id, news_id, title, body, published_at in rows:
        candidates.setdefault(int(cluster_id), []).append(
            {
                "_news_id": int(news_id),
                "title": title or "",
                "text": body or "",
                "published_at": published_at,
            }
        )

    return candidates


def promote_cluster(cluster_id: int, news_id: int) -> bool:
    """대표 기사를 설정한다. 본문은 수집 때 이미 저장돼 있어 건드리지 않는다.

    이미 대표가 있으면(다른 런이 먼저 승격했다) 아무것도 쓰지 않고 False 다. news_id 가 이
    클러스터의 기사가 아니면 ValueError 다.
    """

    with session_scope() as session:
        member = session.execute(
            SELECT_CLUSTER_MEMBER_SQL, {"cluster_id": cluster_id, "news_id": news_id}
        ).first()
        if member is None:
            raise ValueError(f"클러스터 {cluster_id} 의 기사가 아님: news_id={news_id}")

        claimed = session.execute(
            SET_CLUSTER_REPRESENTATIVE_SQL, {"cluster_id": cluster_id, "news_id": news_id}
        ).rowcount

    return bool(claimed)


def fetch_untitled_promoted(promote_size: int) -> list[int]:
    """승격됐는데 제목이 없는 클러스터. 제목 입력은 fetch_cluster_candidates 로 읽는다."""

    with session_scope() as session:
        rows = session.execute(
            SELECT_UNTITLED_PROMOTED_SQL, {"promote_size": promote_size}
        ).fetchall()

    return [int(row[0]) for row in rows]


def count_pending_representatives(promote_size: int) -> int:
    """삼중항 추출을 아직 시도하지 않은(triple_extracted IS NULL), 승격된 클러스터의 대표 기사 수"""

    with session_scope() as session:
        return int(
            session.execute(
                SELECT_PENDING_REPRESENTATIVES_SQL, {"promote_size": promote_size}
            ).scalar_one()
        )
