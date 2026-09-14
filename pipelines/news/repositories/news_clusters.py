import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.clustering.incremental import (
    ClusterAssignment,
    ClusterSeed,
    Terms,
    merge_term_weights,
    pick_representative,
    sum_terms,
    top_keywords,
)

# first_published_at 이 [start, end] 안인 클러스터와 그 프로필. last_stored_date 는 저장된
# 멤버의 최신 발행일(KST)
SELECT_ACTIVE_CLUSTER_SEEDS_SQL = text(
    """
    SELECT nc.id,
           nc.term_weights,
           nc.original_size,
           nc.member_count,
           (
             SELECT MAX((n.published_at AT TIME ZONE 'Asia/Seoul')::date)
               FROM news n
              WHERE n.cluster_id = nc.id
           ) AS last_stored_date,
           nc.first_published_at
      FROM news_clusters nc
     WHERE nc.first_published_at BETWEEN :window_start AND :window_end
     ORDER BY nc.id ASC;
    """
)

# 클러스터 멤버 기사의 토큰 가중치 (대표 재계산용)
SELECT_CLUSTER_MEMBER_TERMS_SQL = text(
    """
    SELECT id, cluster_terms
      FROM news
     WHERE cluster_id = :cluster_id
     ORDER BY id ASC;
    """
)

# 멤버 기사에 클러스터와 토큰 가중치 기록
UPDATE_NEWS_CLUSTER_MEMBER_SQL = text(
    """
    UPDATE news
       SET cluster_id = :cluster_id,
           cluster_terms = CAST(:terms AS jsonb)
     WHERE id = :news_id;
    """
)

# 새 뉴스 클러스터 생성
INSERT_NEWS_CLUSTER_SQL = text(
    """
    INSERT INTO news_clusters (
        representative_news_id, keywords, term_weights, cohesion,
        original_size, member_count, first_published_at, last_published_at
    )
    VALUES (
        :representative_news_id, CAST(:keywords AS text[]),
        CAST(:term_weights AS jsonb), :cohesion,
        :original_size, :member_count, :first_published_at, :last_published_at
    )
    RETURNING id;
    """
)

# 이름이 없고 판정 기사 수가 기준을 넘은 클러스터 중 이번 런에 만들어지거나 갱신된 것
SELECT_UNTITLED_CLUSTER_IDS_SQL = text(
    """
    SELECT id
      FROM news_clusters
     WHERE title IS NULL
       AND original_size >= :min_size
       AND member_count >= 1
       AND updated_at >= :since
     ORDER BY id;
    """
)

# 클러스터 멤버 기사 (제목 생성 입력). 보도 시각 순
SELECT_CLUSTER_ARTICLES_SQL = text(
    """
    SELECT cluster_id, title, text, published_at
      FROM news
     WHERE cluster_id = ANY(:cluster_ids)
     ORDER BY cluster_id, published_at ASC NULLS LAST, id ASC;
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

# 시드 클러스터에 배치 판정 결과 누적. {representative_assignment} 는 대표가 바뀔 때만
# "representative_news_id = :representative_news_id, cohesion = :cohesion," 이 된다.
UPDATE_NEWS_CLUSTER_SQL = """
    UPDATE news_clusters
       SET term_weights = CAST(:term_weights AS jsonb),
           keywords = CAST(:keywords AS text[]),
           original_size = original_size + :original_size_delta,
           member_count = member_count + :member_delta,
           first_published_at = LEAST(first_published_at, :first_published_at),
           last_published_at = GREATEST(last_published_at, :last_published_at),
           {representative_assignment}
           updated_at = now()
     WHERE id = :cluster_id;
"""


@dataclass(frozen=True)
class ClusterArticle:
    """제목 생성 입력용 멤버 기사 스냅샷 (news 한 행)."""

    title: str
    text: str
    published_at: datetime | None


def fetch_untitled_cluster_ids(min_size: int, since: datetime) -> list[int]:
    """since 이후 만들어지거나 갱신됐고, 판정 기사 수가 min_size 이상인데 이름이 없는 클러스터"""

    with session_scope() as session:
        rows = session.execute(
            SELECT_UNTITLED_CLUSTER_IDS_SQL, {"min_size": min_size, "since": since}
        ).fetchall()

    return [int(row[0]) for row in rows]


def fetch_cluster_articles(cluster_ids: list[int]) -> dict[int, list[ClusterArticle]]:
    """클러스터별 멤버 기사 목록 (보도 시각 순)"""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_ARTICLES_SQL, {"cluster_ids": [int(c) for c in cluster_ids]}
        ).fetchall()

    articles: dict[int, list[ClusterArticle]] = {}
    for cluster_id, title, body, published_at in rows:
        articles.setdefault(int(cluster_id), []).append(
            ClusterArticle(title=title or "", text=body or "", published_at=published_at)
        )

    return articles


def update_cluster_title(cluster_id: int, title: str) -> None:
    with session_scope() as session:
        session.execute(UPDATE_CLUSTER_TITLE_SQL, {"cluster_id": cluster_id, "title": title})


def fetch_active_cluster_seeds(window_start: datetime, window_end: datetime) -> list[ClusterSeed]:
    """
    윈도우 기간 내 활성 클러스터 조회 — first_published_at 이 [window_start, window_end] 안인 것
    """

    with session_scope() as session:
        rows = session.execute(
            SELECT_ACTIVE_CLUSTER_SEEDS_SQL,
            {"window_start": window_start, "window_end": window_end},
        ).fetchall()

        return [
            ClusterSeed(
                cluster_id=int(row.id),
                term_weights={token: float(weight) for token, weight in row.term_weights.items()},
                original_size=int(row.original_size),
                member_count=int(row.member_count),
                last_stored_date=row.last_stored_date,
                first_published_at=row.first_published_at,
            )
            for row in rows
        ]


def fetch_cluster_member_terms(cluster_id: int) -> list[tuple[int, dict[str, float]]]:
    """클러스터에 저장된 멤버 뉴스들의 (news_id, cluster_terms) 목록. 대표 뉴스 재계산에 사용."""

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_MEMBER_TERMS_SQL, {"cluster_id": cluster_id}
        ).fetchall()

        return [(int(news_id), terms or {}) for news_id, terms in rows]


def _assign_cluster_members(
    session, cluster_id: int, members: list[tuple[int, dict[str, float]]]
) -> None:
    """멤버 기사의 news.cluster_id 와 cluster_terms 를 기록한다."""

    for news_id, terms in members:
        session.execute(
            UPDATE_NEWS_CLUSTER_MEMBER_SQL,
            {
                "cluster_id": cluster_id,
                "terms": json.dumps(terms, ensure_ascii=False),
                "news_id": news_id,
            },
        )


def create_news_cluster(
    *,
    representative_news_id: int,
    cohesion: float,
    term_weights: dict[str, float],
    keywords: list[str],
    original_size: int,
    first_published_at: datetime,
    last_published_at: datetime,
    members: list[tuple[int, dict[str, float]]],
) -> int:
    """
    새 클러스터 INSERT

    news_cluster 테이블에 행을 하나 추가하고 새 id를 받은 뒤, 멤버 news들의 news.cluster_id와
    cluster_terms를 채운다.
    대표 news는 첫 번째, 즉 저장에 성공한 메도이드이다.
    """

    with session_scope() as session:
        cluster_id = session.execute(
            INSERT_NEWS_CLUSTER_SQL,
            {
                "representative_news_id": representative_news_id,
                "keywords": keywords,
                "term_weights": json.dumps(term_weights, ensure_ascii=False),
                "cohesion": cohesion,
                "original_size": original_size,
                "member_count": len(members),
                "first_published_at": first_published_at,
                "last_published_at": last_published_at,
            },
        ).scalar_one()

        _assign_cluster_members(session, int(cluster_id), members)

        return int(cluster_id)


def update_news_cluster(
    cluster_id: int,
    *,
    term_weights: dict[str, float],
    keywords: list[str],
    original_size_delta: int,
    first_published_at: datetime,
    last_published_at: datetime,
    representative_news_id: int | None,
    cohesion: float | None,
    members: list[tuple[int, dict[str, float]]],
) -> None:
    """
    기존 클러스터 UPDATE

    news_clusters 행을 누적 갱신합니다.
    프로필과 키워드는 교체, original_size와 member_count는 증가, 시간 범위는 LEAST/GREATEST로
    넓힙니다.
    대표와 응집도는 representative_news_id가 넘어왔을 때만 바꿉니다.
    저장된 신규 멤버가 없으면 그대로 둡니다.
    그리고 새 멤버의 news.cluster_id를 채웁니다.
    """

    representative_assignment = ""
    params: dict[str, Any] = {
        "cluster_id": cluster_id,
        "term_weights": json.dumps(term_weights, ensure_ascii=False),
        "keywords": keywords,
        "original_size_delta": original_size_delta,
        "member_delta": len(members),
        "first_published_at": first_published_at,
        "last_published_at": last_published_at,
    }

    if representative_news_id is not None:
        representative_assignment = (
            "representative_news_id = :representative_news_id, cohesion = :cohesion,"
        )
        params["representative_news_id"] = representative_news_id
        params["cohesion"] = cohesion

    with session_scope() as session:
        session.execute(
            text(
                UPDATE_NEWS_CLUSTER_SQL.format(representative_assignment=representative_assignment)
            ),
            params,
        )

        _assign_cluster_members(session, cluster_id, members)


def _record_seed_update(
    assignment: ClusterAssignment,
    members: list[tuple[int, dict[str, float]]],
    survivor_terms: list[Terms],
    keyword_count: int,
) -> None:
    """
    시드 갱신 전처리

    기존 클러스터에 이번 배치를 더할 때 DB에 쓰기 전 값을 준비합니다.
    이번에 저장된 멤버가 있으면 기존 멤버와 합쳐 메도이드를 다시 골라 대표와 응집도를 새로
    계산하고, 프로필(term_weights)을 병합하고, 키워드를 다시 뽑습니다.
    그다음 update_news_cluster를 부릅니다.
    밑줄이 붙은 건 모듈 밖에서 부를 일이 없어서입니다.
    시드 클러스터에 이번 배치의 판정을 더한다.
    저장된 멤버가 있으면 대표를 다시 고른다.
    """

    seed = assignment.seed
    assert seed is not None
    representative_news_id: int | None = None
    cohesion: float | None = None

    if members:
        # 저장된 멤버 전체(기존 + 이번)로 메도이드를 다시 고른다.
        existing = fetch_cluster_member_terms(seed.cluster_id)
        pool_ids = [news_id for news_id, _ in existing] + [news_id for news_id, _ in members]
        pool_terms = [list(terms.items()) for _, terms in existing] + survivor_terms
        representative_index, cohesion = pick_representative(pool_terms)
        representative_news_id = pool_ids[representative_index]

    merged = merge_term_weights(seed.term_weights, assignment.term_weights)
    update_news_cluster(
        seed.cluster_id,
        term_weights=merged,
        keywords=top_keywords(merged, keyword_count),
        original_size_delta=len(assignment.members),
        first_published_at=assignment.first_published_at,
        last_published_at=assignment.last_published_at,
        representative_news_id=representative_news_id,
        cohesion=cohesion,
        members=members,
    )


def record_cluster_assignments(
    assignments: list[ClusterAssignment],
    items: list[dict[str, Any]],
    documents: list[Terms],
    keyword_count: int,
) -> dict[str, int]:
    """저장이 끝난 뒤 판정 결과를 news_clusters 와 news.cluster_id 에 기록한다.

    저장에 성공한 기사(_news_id 가 있고 기존 행 스킵이 아닌 것)만 멤버가 된다. 새 클러스터는
    저장된 기사가 하나도 없으면 만들지 않는다. 시드 클러스터는 저장된 기사가 없어도 프로필과
    판정 수, 시간 범위는 갱신한다 — 버린 기사도 같은 사건이라는 판정 자체는 유효하다.

    클러스터 하나가 트랜잭션 하나다. 실패한 클러스터는 건너뛰고 세어 돌려주며, 그 기사들은
    news 에 cluster_id 없이 남는다(news 저장은 이미 커밋됐다).
    """

    created = 0
    updated = 0
    failed = 0

    for assignment in assignments:
        survivors = [
            index
            for index in assignment.kept
            if items[index].get("_news_id")
            and items[index].get("_save_action") != "skipped_existing"
        ]
        members = [
            (int(items[index]["_news_id"]), sum_terms(documents[index])) for index in survivors
        ]

        if assignment.seed is None and not members:
            continue

        try:
            if assignment.seed is None:
                # kept 는 메도이드가 첫 번째이므로, 저장에 성공한 첫 기사가 대표다.
                create_news_cluster(
                    representative_news_id=members[0][0],
                    cohesion=assignment.cohesion,
                    term_weights=assignment.term_weights,
                    keywords=top_keywords(assignment.term_weights, keyword_count),
                    original_size=len(assignment.members),
                    first_published_at=assignment.first_published_at,
                    last_published_at=assignment.last_published_at,
                    members=members,
                )
                created += 1
            else:
                _record_seed_update(
                    assignment, members, [documents[index] for index in survivors], keyword_count
                )
                updated += 1
        except Exception as e:
            failed += 1
            logging.error(
                "클러스터 기록 실패(건너뜀): "
                f"cluster_id={assignment.seed.cluster_id if assignment.seed else None}, "
                f"news_ids={[news_id for news_id, _ in members]}, error={type(e).__name__}: {e}"
            )

    return {"created": created, "updated": updated, "failed": failed}
