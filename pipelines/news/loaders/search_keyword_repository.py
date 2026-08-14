import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.database import session_scope


def fetch_active_search_keywords(limit: int = 50) -> list[dict[str, Any]]:
    query = """
        SELECT id, keyword
        FROM search_keywords
        WHERE keyword IS NOT NULL
          AND BTRIM(keyword) <> ''
        ORDER BY last_searched_at ASC NULLS FIRST, id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        return [
            {
                "id": int(row[0]),
                "keyword": row[1],
            }
            for row in rows
        ]


def mark_keywords_searched(keyword_ids: list[int]) -> int:

    unique_ids = sorted({int(keyword_id) for keyword_id in keyword_ids if keyword_id})

    if not unique_ids:
        return 0

    with session_scope() as session:
        session.execute(
            text("UPDATE search_keywords SET last_searched_at = now() WHERE id = ANY(:ids);"),
            {"ids": unique_ids},
        )

    logging.info("검색 키워드 last_searched_at 갱신: %d개", len(unique_ids))

    return len(unique_ids)


def mark_news_source_type(news_ids: list[int], source_type: str) -> int:

    unique_ids = sorted({int(news_id) for news_id in news_ids if news_id})

    if not unique_ids:
        return 0

    with session_scope() as session:
        result = session.execute(
            text(
                """
                UPDATE news
                SET source_type = :source_type
                WHERE id = ANY(:ids)
                  AND (source_type IS NULL OR BTRIM(source_type) = '');
                """
            ),
            {"source_type": source_type, "ids": unique_ids},
        )
        updated_count = result.rowcount

    logging.info("news.source_type='%s' 기록: %d개", source_type, updated_count)

    return updated_count
