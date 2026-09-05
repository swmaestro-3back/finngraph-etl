"""news_clusters 로더 — 배치 간 클러스터 원장의 조회·기록.

판정 로직은 pipelines/news/transformers/clustering/incremental.py 에 있고, 여기는 그 입력
(시드)과 출력(클러스터 행, news.cluster_id / cluster_terms)의 DB 접근만 담당한다.
ClusterSeed 는 incremental 모듈에서 직접 가져온다 — 패키지 barrel 은 Kiwi 를 import 하는
preprocess 까지 끌어와 로더만 쓰는 job 이 무거워진다.
"""

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.clustering.incremental import ClusterSeed


def fetch_active_cluster_seeds(window_start: datetime) -> list[ClusterSeed]:
    """윈도우 안(last_published_at >= window_start) 클러스터를 판정 시드로 읽는다.

    last_stored_date 는 저장된 멤버(news.cluster_id)만으로 센다 — cap 의 날짜 예외는
    "마지막으로 저장한 기사" 기준이고, last_published_at 은 버린 기사까지 반영한 값이다.
    """

    query = """
        SELECT
            nc.id,
            nc.term_weights,
            nc.original_size,
            nc.member_count,
            (
                SELECT MAX((n.published_at AT TIME ZONE 'Asia/Seoul')::date)
                FROM news n
                WHERE n.cluster_id = nc.id
            ) AS last_stored_date
        FROM news_clusters nc
        WHERE nc.last_published_at >= :window_start
        ORDER BY nc.id ASC;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"window_start": window_start}).fetchall()

        return [
            ClusterSeed(
                cluster_id=int(cluster_id),
                term_weights={token: float(weight) for token, weight in term_weights.items()},
                original_size=int(original_size),
                member_count=int(member_count),
                last_stored_date=last_stored_date,
            )
            for cluster_id, term_weights, original_size, member_count, last_stored_date in rows
        ]


def fetch_cluster_member_terms(cluster_id: int) -> list[tuple[int, dict[str, float]]]:
    """클러스터에 저장된 멤버의 (news_id, cluster_terms) 목록. 대표 재계산에 쓴다."""

    query = """
        SELECT id, cluster_terms
        FROM news
        WHERE cluster_id = :cluster_id
        ORDER BY id ASC;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"cluster_id": cluster_id}).fetchall()

        return [(int(news_id), terms or {}) for news_id, terms in rows]


def _assign_cluster_members(
    session, cluster_id: int, members: list[tuple[int, dict[str, float]]]
) -> None:
    """멤버 기사의 news.cluster_id 와 cluster_terms 를 기록한다."""

    for news_id, terms in members:
        session.execute(
            text(
                """
                UPDATE news
                SET cluster_id = :cluster_id,
                    cluster_terms = CAST(:terms AS jsonb)
                WHERE id = :news_id;
                """
            ),
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
    """news_clusters 행을 만들고 멤버의 news.cluster_id / cluster_terms 를 채운다.

    새 클러스터 id 를 돌려준다.

    members 는 (news_id, 그 기사의 토큰 가중치)이고 대표는 그 안에 있어야 한다. news 행이
    먼저 저장돼 있어야 대표 FK 가 성립하므로 save_news_items 뒤에 부른다.
    """

    with session_scope() as session:
        cluster_id = session.execute(
            text(
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
            ),
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
    """기존 클러스터에 이번 배치의 판정 결과를 더한다.

    term_weights 는 병합이 끝난 전체 프로필이고, original_size 는 판정된 기사 수만큼,
    member_count 는 members 수만큼 는다. 대표와 응집도는 representative_news_id 가 있을
    때만 바꾼다 — 저장된 신규 멤버가 없으면(전원 cap 또는 본문 실패) 그대로 둔다.
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
                f"""
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
            ),
            params,
        )

        _assign_cluster_members(session, cluster_id, members)
