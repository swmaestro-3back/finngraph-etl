import json
import logging
from dataclasses import dataclass
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
from pipelines.news.transformers.issue_linker import ClusterMember, LinkCandidate

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


# ── 이슈 타임라인 ────────────────────────────────────────────────────────────
# 판정 규칙은 transformers/issue_linker.py 에, 흐름은 jobs/link_issues.py 에 있다. pgvector 파이썬
# 패키지를 쓰지 않으므로 벡터는 텍스트 표현('[x,y,...]')으로 주고받고 SQL 에서 CAST 한다.
#
# 연결 쓰기(임베딩·판정·초기화)는 updated_at 을 건드리지 않는다. 승격·제목 재시도와 Event 스캔
# (events/repositories/postgres/news_clusters.py)이 updated_at 범위로 대상을 고르므로, 연결을 쓰면서
# 옛 클러스터를 다시 그 대상으로 만들면 안 된다.

# 연결 대상은 이름과 대표 기사가 있고 아직 판정 전인 클러스터다. 대표 기사에 요약(포인트나 문단)이
# 있으면 바로 잇고, 없으면 클러스터의 updated_at 이 summary_deadline 보다 이를 때만 요약 없이
# 잇는다. updated_at 은 승격·제목 생성 때 갱신되므로, 그 값이 summary_deadline 보다 이르면 이름이
# 붙은 뒤 요약을 다시 시도할 시간이 충분히 지났다는 뜻이다. 요약이 계속 실패하는 기사가 연결을
# 무기한 막지 않게 하려는 조건이다.
_LINK_TARGET_FROM_SQL = """
      FROM news_clusters c
      JOIN news n ON n.id = c.representative_news_id
     WHERE c.title IS NOT NULL
       AND c.representative_news_id IS NOT NULL
       AND c.linked_at IS NULL
       AND c.first_published_at >= :since
"""

_SUMMARY_READY_SQL = """(
           n.summary_points IS NOT NULL
        OR NULLIF(BTRIM(n.summary), '') IS NOT NULL
        OR c.updated_at < :summary_deadline
       )"""

# 오래된 것부터 고른다. 판정마다 커밋하므로 앞선 대상이 같은 실행에서 뒤에 오는 대상의 부모
# 후보가 된다.
SELECT_LINK_TARGETS_SQL = text(
    f"""
    SELECT c.id,
           c.title,
           c.first_published_at,
           c.embedding::text AS embedding,
           n.summary,
           n.summary_points
    {_LINK_TARGET_FROM_SQL}
       AND {_SUMMARY_READY_SQL}
     ORDER BY c.first_published_at ASC, c.id ASC
     LIMIT :limit;
    """
)

COUNT_LINK_TARGETS_SQL = text(
    f"""
    SELECT COUNT(*) FILTER (WHERE ready) AS targets,
           COUNT(*) FILTER (WHERE ready AND without_embedding) AS without_embedding,
           COUNT(*) FILTER (WHERE NOT ready) AS waiting_summary
      FROM (
        SELECT {_SUMMARY_READY_SQL} AS ready,
               c.embedding IS NULL AS without_embedding
        {_LINK_TARGET_FROM_SQL}
      ) t;
    """
)

# 클러스터 멤버 기사(승격 전후에 들어온 기사 모두)의 제목과 연결 기업을 읽는다. 임베딩 텍스트에
# 넣을 기사 제목과 주요 기업(제목에 나온 기업) 판정의 입력이다. 순서는 후보 기사(승격 전에 들어온
# 기사, cluster_terms 가 있는 행)가 먼저이고, 그 안에서는 발행 시각순이다. 후보 기사는 승격 전에 다
# 차고 그 뒤로 바뀌지 않는다. 그래서 임베딩 텍스트의 앞 제목은 승격 직후의 스케줄 연결에서
# 만들든, 기사가 더 붙은 뒤의 백필에서 만들든 같다. 최신순이면 만드는 시점에 따라 텍스트가 달라져,
# 같은 이슈라도 코사인이 달라지고 임계값과 어긋난다.
SELECT_CLUSTER_MEMBERS_SQL = text(
    """
    SELECT n.cluster_id,
           n.title,
           COALESCE(
             ARRAY_AGG(x.company_id) FILTER (WHERE x.company_id IS NOT NULL),
             '{}'
           ) AS company_ids
      FROM news n
      LEFT JOIN news_companies x ON x.news_id = n.id
     WHERE n.cluster_id = ANY(:cluster_ids)
     GROUP BY n.cluster_id, n.id
     ORDER BY n.cluster_id, n.cluster_terms IS NULL, n.published_at ASC NULLS LAST, n.id ASC;
    """
)

UPDATE_CLUSTER_EMBEDDING_SQL = text(
    """
    UPDATE news_clusters
       SET embedding = CAST(:embedding AS vector)
     WHERE id = :cluster_id;
    """
)

# 부모 후보는 연결 판정이 끝났고, 대상보다 먼저 시작했으며, 대상 시작 전 lookback 안에 있는
# 클러스터다. "먼저"는 대상 처리 순서와 같은 (first_published_at, id) 순서로 비교한다. 부모의
# story_root_id 를 물려받아야 하므로 판정이 끝난 것만 본다. {company_filter} 는 대상에 기업이 있으면
# 대상의 주요 기업 중 하나라도 연결된 클러스터를, 없으면 기업이 하나도 없는 클러스터를 고른다. 후보
# 쪽의 주요 기업 규칙은 제목 매치가 필요해 jobs/link_issues.py 가 다시 거른다. 정렬은
# issue_linker.choose_parent 와 같다.
#
# 파이썬에서 탈락하는 후보가 있으므로 페이지로 나눠 읽는다. :after_score 가 있으면 지난 페이지의
# 마지막 행 (score, first_published_at, id) 뒤부터 읽는다. 세 열 모두 내림차순이라 행 비교 하나로
# 이어 읽을 수 있다. score 는 float8 이고 같은 입력이면 같은 값이 나오므로, 파이썬으로 받았다가 다시
# 넘겨도 경계가 어긋나지 않는다.
_LINK_CANDIDATES_SQL = """
    SELECT s.id, s.first_published_at, s.score
      FROM (
        SELECT c.id,
               c.first_published_at,
               1 - (c.embedding <=> CAST(:embedding AS vector)) AS score
          FROM news_clusters c
         WHERE c.linked_at IS NOT NULL
           AND c.embedding IS NOT NULL
           AND (c.first_published_at, c.id)
               < (CAST(:first_published_at AS TIMESTAMPTZ), CAST(:cluster_id AS BIGINT))
           AND c.first_published_at >= :window_start
           AND {company_filter}
      ) s
     WHERE s.score >= :min_score
       AND (
             CAST(:after_score AS DOUBLE PRECISION) IS NULL
          OR (s.score, s.first_published_at, s.id) < (
               CAST(:after_score AS DOUBLE PRECISION),
               CAST(:after_published_at AS TIMESTAMPTZ),
               CAST(:after_id AS BIGINT)
             )
           )
     ORDER BY s.score DESC, s.first_published_at DESC, s.id DESC
     LIMIT :limit;
"""

SELECT_LINK_CANDIDATES_SHARING_COMPANY_SQL = text(
    _LINK_CANDIDATES_SQL.format(
        company_filter="""EXISTS (
               SELECT 1
                 FROM news n
                 JOIN news_companies x ON x.news_id = n.id
                WHERE n.cluster_id = c.id
                  AND x.company_id = ANY(:company_ids)
           )"""
    )
)

SELECT_LINK_CANDIDATES_WITHOUT_COMPANY_SQL = text(
    _LINK_CANDIDATES_SQL.format(
        company_filter="""NOT EXISTS (
               SELECT 1
                 FROM news n
                 JOIN news_companies x ON x.news_id = n.id
                WHERE n.cluster_id = c.id
           )"""
    )
)

# 부모가 있으면 부모의 story_root_id 를 물려받고, 없으면 자기 자신이 루트(부모가 없는 타임라인 첫
# 이슈)다. 첫 판정은 linked_at IS NULL 조건으로 같은 클러스터를 두 번 판정하지 않는다. 재판정은
# 판정된 클러스터만 덮어쓰므로, 그사이 백필 초기화가 지운 클러스터를 되살리지 않는다.
_UPDATE_LINK_DECISION_SQL = """
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
           link_relation = CAST(:link_relation AS TEXT),
           linked_at = now()
     WHERE id = :cluster_id
       AND linked_at IS {linked};
"""

UPDATE_LINK_DECISION_SQL = text(_UPDATE_LINK_DECISION_SQL.format(linked="NULL"))
UPDATE_RELINK_DECISION_SQL = text(_UPDATE_LINK_DECISION_SQL.format(linked="NOT NULL"))

# 재판정 대상은 판정된 클러스터 중 정렬상 (after_published_at, after_id) 뒤에 시작했고
# relink_since 이후에 시작한 것이며, 오래된 순으로 읽는다. 부모는 늘 정렬상 앞에 있어서 이 구간에
# 속한 클러스터의 자식도 모두 이 구간 안에 있다. 그래서 구간을 앞에서부터 다시 판정하면
# story_root_id 가 어긋나지 않는다.
SELECT_RELINK_TAIL_SQL = text(
    """
    SELECT c.id,
           c.title,
           c.first_published_at,
           c.embedding::text AS embedding,
           c.parent_cluster_id
      FROM news_clusters c
     WHERE c.linked_at IS NOT NULL
       AND c.embedding IS NOT NULL
       AND (c.first_published_at, c.id)
           > (CAST(:after_published_at AS TIMESTAMPTZ), CAST(:after_id AS BIGINT))
       AND c.first_published_at >= :relink_since
     ORDER BY c.first_published_at ASC, c.id ASC;
    """
)

# 연결을 처음부터 다시 만들 때(백필 DAG 의 reset=true) 지우는 범위다. 부모는 늘 더 먼저 시작한
# 클러스터라, first_published_at 이 since 이후인 구간을 통째로 지우면 그 앞의 판정은 지워진
# 클러스터를 가리키지 않는다. 임베딩도 지워 지금의 요약·설정으로 다시 만든다.
_RESETTABLE_SQL = """
      FROM news_clusters
     WHERE first_published_at >= :since
       AND (linked_at IS NOT NULL OR embedding IS NOT NULL)
"""

COUNT_RESETTABLE_SQL = text(
    f"""
    SELECT COUNT(*) AS clusters,
           COUNT(*) FILTER (WHERE linked_at IS NOT NULL) AS linked
    {_RESETTABLE_SQL};
    """
)

RESET_LINKS_SQL = text(
    """
    UPDATE news_clusters
       SET embedding = NULL,
           parent_cluster_id = NULL,
           story_root_id = NULL,
           link_score = NULL,
           link_relation = NULL,
           linked_at = NULL
     WHERE first_published_at >= :since
       AND (linked_at IS NOT NULL OR embedding IS NOT NULL);
    """
)


@dataclass(frozen=True)
class LinkTarget:
    """연결 판정 대상 이슈를 담는다. summary·summary_points 는 대표 기사의 것이고, embedding 은
    pgvector 텍스트 표현이며 아직 없으면 None 이다. parent_id 는 재판정 대상의 현재 부모다(첫
    판정 대상과 루트는 None)."""

    cluster_id: int
    title: str
    first_published_at: datetime
    summary: str | None = None
    summary_points: Any = None
    embedding: str | None = None
    parent_id: int | None = None


def fetch_link_targets(since: datetime, limit: int, summary_deadline: datetime) -> list[LinkTarget]:
    """이름과 대표 기사가 있고 판정 전이며 first_published_at 이 since 이후인 클러스터를 오래된
    순으로 읽는다.

    대표 기사에 요약이 없는 클러스터는 updated_at 이 summary_deadline 보다 이를 때만 넣는다.
    """

    with session_scope() as session:
        rows = session.execute(
            SELECT_LINK_TARGETS_SQL,
            {"since": since, "limit": limit, "summary_deadline": summary_deadline},
        ).fetchall()

    return [
        LinkTarget(
            cluster_id=int(row.id),
            title=row.title,
            first_published_at=row.first_published_at,
            summary=row.summary,
            summary_points=row.summary_points,
            embedding=row.embedding,
        )
        for row in rows
    ]


def count_link_targets(since: datetime, summary_deadline: datetime) -> dict[str, int]:
    """연결 대상 수(targets), 그중 임베딩이 없는 수, 대표 기사 요약을 기다리느라 빠진 수를 센다."""

    with session_scope() as session:
        row = session.execute(
            COUNT_LINK_TARGETS_SQL, {"since": since, "summary_deadline": summary_deadline}
        ).one()

    return {
        "targets": int(row.targets),
        "without_embedding": int(row.without_embedding),
        "waiting_summary": int(row.waiting_summary),
    }


def fetch_cluster_members(cluster_ids: list[int]) -> dict[int, list[ClusterMember]]:
    """클러스터별 멤버 기사의 제목과 연결 기업을 후보 기사 먼저, 발행 시각순으로 읽는다. 멤버가
    없는 클러스터는 결과에 키가 없다."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_MEMBERS_SQL, {"cluster_ids": [int(c) for c in cluster_ids]}
        ).fetchall()

    members: dict[int, list[ClusterMember]] = {}
    for cluster_id, title, company_ids in rows:
        members.setdefault(int(cluster_id), []).append(
            ClusterMember(
                title=title or "",
                company_ids=frozenset(int(c) for c in company_ids or []),
            )
        )

    return members


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
    window_start: datetime,
    min_score: float,
    limit: int,
    after: LinkCandidate | None = None,
) -> list[LinkCandidate]:
    """대상의 부모 후보를 코사인 내림차순으로 limit 개 읽는다. 기업 조건과 min_score 는 SQL 에서
    먼저 거른다. after 를 주면 그 후보(지난 페이지의 마지막 행) 다음부터 읽는다.

    돌려주는 후보의 company_ids 는 비어 있고, 후보의 주요 기업은 jobs/link_issues.py 가 멤버
    제목으로 채운다.
    """

    params: dict[str, Any] = {
        "cluster_id": cluster_id,
        "embedding": embedding,
        "first_published_at": first_published_at,
        "window_start": window_start,
        "min_score": min_score,
        "limit": limit,
        "after_score": None if after is None else after.score,
        "after_published_at": None if after is None else after.first_published_at,
        "after_id": None if after is None else after.cluster_id,
    }
    if company_ids:
        sql = SELECT_LINK_CANDIDATES_SHARING_COMPANY_SQL
        params["company_ids"] = sorted(company_ids)
    else:
        sql = SELECT_LINK_CANDIDATES_WITHOUT_COMPANY_SQL

    with session_scope() as session:
        rows = session.execute(sql, params).fetchall()

    return [
        LinkCandidate(
            cluster_id=int(row.id),
            first_published_at=row.first_published_at,
            score=float(row.score),
        )
        for row in rows
    ]


def fetch_relink_tail(
    after_published_at: datetime, after_id: int, relink_since: datetime
) -> list[LinkTarget]:
    """판정된 클러스터 중 정렬상 (after_published_at, after_id) 뒤에 시작했고 relink_since 이후에
    시작한 것을 오래된 순으로 읽는다. 임베딩이 이미 있으므로 summary 는 채우지 않는다."""

    with session_scope() as session:
        rows = session.execute(
            SELECT_RELINK_TAIL_SQL,
            {
                "after_published_at": after_published_at,
                "after_id": after_id,
                "relink_since": relink_since,
            },
        ).fetchall()

    return [
        LinkTarget(
            cluster_id=int(row.id),
            title=row.title,
            first_published_at=row.first_published_at,
            embedding=row.embedding,
            parent_id=None if row.parent_cluster_id is None else int(row.parent_cluster_id),
        )
        for row in rows
    ]


def record_link_decision(
    cluster_id: int,
    parent_id: int | None,
    score: float | None,
    relation: str | None,
    *,
    relink: bool = False,
) -> bool:
    """연결 판정을 쓴다. 부모가 없으면 루트로 기록하고, 쓴 행이 없으면 False 를 돌려준다.

    첫 판정(relink=False)은 판정 전 클러스터에만, 재판정(relink=True)은 판정된 클러스터에만 쓴다.
    """

    with session_scope() as session:
        updated = session.execute(
            UPDATE_RELINK_DECISION_SQL if relink else UPDATE_LINK_DECISION_SQL,
            {
                "cluster_id": cluster_id,
                "parent_id": parent_id,
                # vector 가 float4 라 코사인의 끝자리는 의미가 없으므로 소수 6자리까지만 남긴다.
                "link_score": None if score is None else round(score, 6),
                "link_relation": relation,
            },
        ).rowcount

    return updated > 0


def count_resettable_clusters(since: datetime) -> dict[str, int]:
    """first_published_at 이 since 이후이고 임베딩이나 판정이 있는 클러스터 수와 그중 판정된 수를
    센다."""

    with session_scope() as session:
        row = session.execute(COUNT_RESETTABLE_SQL, {"since": since}).one()

    return {"clusters": int(row.clusters), "linked": int(row.linked)}


def reset_cluster_links(since: datetime) -> int:
    """since 이후 시작한 클러스터의 임베딩과 연결 판정을 지우고, 지운 클러스터 수를 돌려준다."""

    with session_scope() as session:
        return int(session.execute(RESET_LINKS_SQL, {"since": since}).rowcount)
