"""entities_relations 뷰(근거 원장 집계) 조회.

Neo4j 간선 요약을 쓸 때 읽는다. 뷰의 원천인 relation_sources 쓰기는 relation_sources.py.
공시 파이프라인(link_supply_contracts)도 간선 요약 동기화에 fetch_edge_summaries를 같이
쓴다 — 그래프 간선 어휘의 소유자가 triples 도메인이라 여기 둔다.
"""

from __future__ import annotations

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 간선 자연키 목록에 대한 집계 행 — Neo4j 간선 속성의 원천.
SELECT_EDGE_SUMMARIES_SQL = text(
    """
    SELECT subject_name, relation, object_name,
           news_mention_count, disclosure_count,
           first_mentioned_at, last_mentioned_at,
           disclosure_rcept_nos, disclosure_items,
           news_ids, news_items
      FROM entities_relations
     WHERE (subject_name, relation, object_name) IN (
           SELECT * FROM unnest(
               CAST(:subject_names AS text[]),
               CAST(:relations     AS text[]),
               CAST(:object_names  AS text[])
           ))
    """
)


def fetch_edge_summaries(keys: list[tuple[str, str, str]]) -> list[dict]:
    """(subject_name, relation, object_name) 목록의 간선 요약을 뷰에서 조회한다.

    반환 dict는 repositories/neo4j/relations.py의 sync_edge_summaries 입력 형식과 같다.
    아직 원장에 근거가 없는 키는 결과에 없다.
    """

    unique_keys = sorted(set(keys))

    if not unique_keys:
        return []

    with session_scope() as session:
        rows = session.execute(
            SELECT_EDGE_SUMMARIES_SQL,
            {
                "subject_names": [key[0] for key in unique_keys],
                "relations": [key[1] for key in unique_keys],
                "object_names": [key[2] for key in unique_keys],
            },
        ).fetchall()

    return [
        {
            "subject_name": row.subject_name,
            "relation": row.relation,
            "object_name": row.object_name,
            "news_mention_count": int(row.news_mention_count),
            "disclosure_count": int(row.disclosure_count),
            "first_mentioned_at": row.first_mentioned_at,
            "last_mentioned_at": row.last_mentioned_at,
            # 해당 출처 근거가 없는 간선은 뷰가 NULL 을 주므로 빈 배열로 정규화한다.
            "disclosure_rcept_nos": list(row.disclosure_rcept_nos or []),
            "disclosure_items": list(row.disclosure_items or []),
            "news_ids": [int(news_id) for news_id in (row.news_ids or [])],
            "news_items": list(row.news_items or []),
        }
        for row in rows
    ]
