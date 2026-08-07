"""관련 뉴스 조회 — `news_relations` self-join 단일 쿼리.

`idx_news_relations_triplet (subject_name, relation, object_name)` 인덱스가
이미 있어 추가 스키마가 필요 없다.

한계 (v1.5 재검토):
  · 조인 키가 `subject_name`/`object_name`이라 표기 흔들림("POSCO홀딩스" vs
    "포스코홀딩스")에 취약하다. `companies` 적재로 code가 채워지면 code 기준
    조인으로 옮겨야 하며, 그때는 표현식 인덱스가 필요하다.
  · 시간창이 기준 뉴스 중심의 대칭 구간이라, 며칠에 걸쳐 연쇄로 전개되는 사건의
    양 끝은 서로를 보지 못한다. 창을 넓히면 대부분 상쇄된다.
"""

from __future__ import annotations

import secrets
from typing import Any

from sqlalchemy import text

from pipelines.common.config import get_settings
from pipelines.common.database import session_scope

DEFAULT_RELATED_NEWS_LIMIT = 5

_RELATED_NEWS_SQL = """
WITH base AS (
    SELECT r.subject_name,
           r.relation,
           r.object_name,
           r.subject_code,
           r.object_code,
           n.published_at AS base_at
    FROM news_relations AS r
    JOIN news AS n ON n.id = r.news_id
    WHERE r.news_id = :news_id
      AND n.published_at IS NOT NULL
),
members AS (
    SELECT b.subject_name,
           b.relation,
           b.object_name,
           COALESCE(b.subject_code, b.subject_name) AS subject_ref,
           COALESCE(b.object_code,  b.object_name)  AS object_ref,
           m.news_id,
           mn.title,
           mn.link,
           mn.published_at,
           row_number() OVER (
               PARTITION BY b.subject_name, b.relation, b.object_name
               ORDER BY md5(mn.id::text || :seed), mn.id
           ) AS rn,
           count(*) OVER (
               PARTITION BY b.subject_name, b.relation, b.object_name
           ) AS news_count,
           min(mn.published_at) OVER (
               PARTITION BY b.subject_name, b.relation, b.object_name
           ) AS first_news_at,
           max(mn.published_at) OVER (
               PARTITION BY b.subject_name, b.relation, b.object_name
           ) AS last_news_at
    FROM base AS b
    JOIN news_relations AS m
           ON m.subject_name = b.subject_name
          AND m.relation     = b.relation
          AND m.object_name  = b.object_name
    JOIN news AS mn ON mn.id = m.news_id
    WHERE mn.published_at
              BETWEEN b.base_at - make_interval(hours => :window_hours)
                  AND b.base_at + make_interval(hours => :window_hours)
)
SELECT subject_ref, relation, object_ref,
       news_count, first_news_at, last_news_at,
       news_id, title, link, published_at
FROM members
-- 자기 자신은 그룹 메타(news_count 등)에는 포함하되 리스트에서는 뺀다.
-- rn 상위 (limit+1)건 중 자기 자신은 최대 1건이므로, 제외 후에도 limit건이 남는다.
WHERE news_id <> :news_id
  AND rn <= :limit_plus_self
ORDER BY last_news_at DESC, subject_ref ASC, relation ASC, object_ref ASC,
         published_at DESC NULLS LAST, news_id DESC;
"""


def _default_seed() -> str:
    """호출마다 새로 생성되는 시드. 상세 페이지를 열 때마다 노출 대상이 바뀐다."""
    return secrets.token_hex(8)


def fetch_related_news_groups(
    news_id: int,
    limit_per_group: int = DEFAULT_RELATED_NEWS_LIMIT,
    window_hours: int | None = None,
    seed: str | None = None,
) -> list[dict[str, Any]]:

    settings = get_settings()
    window = settings.related_news_window_hours if window_hours is None else window_hours

    with session_scope() as session:
        rows = (
            session.execute(
                text(_RELATED_NEWS_SQL),
                {
                    "news_id": news_id,
                    "window_hours": window,
                    "seed": seed if seed is not None else _default_seed(),
                    # 자기 자신이 rn 안에 포함될 수 있으므로 한 칸 여유를 준다.
                    "limit_plus_self": limit_per_group + 1,
                },
            )
            .mappings()
            .all()
        )

    groups: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in rows:
        key = (row["subject_ref"], row["relation"], row["object_ref"])
        group = groups.get(key)

        if group is None:
            group = {
                "subject_ref": row["subject_ref"],
                "relation": row["relation"],
                "object_ref": row["object_ref"],
                "news_count": row["news_count"],
                "first_news_at": row["first_news_at"],
                "last_news_at": row["last_news_at"],
                "news": [],
            }
            groups[key] = group

        if len(group["news"]) < limit_per_group:
            group["news"].append(
                {
                    "news_id": row["news_id"],
                    "title": row["title"],
                    "link": row["link"],
                    "published_at": row["published_at"],
                }
            )

    return list(groups.values())
