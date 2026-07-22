import logging
from typing import Any

from pipelines.news.loaders.news_repository import get_connection


def fetch_active_search_keywords(limit: int = 50) -> list[dict[str, Any]]:

    query = """
        SELECT id, keyword, source_type, source_id
        FROM search_keywords
        WHERE status = 'active'
          AND keyword IS NOT NULL
          AND BTRIM(keyword) <> ''
        ORDER BY last_searched_at ASC NULLS FIRST, id ASC
        LIMIT %s;
    """

    conn = get_connection()

    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(query, (limit,))

            return [
                {
                    "id": int(row[0]),
                    "keyword": row[1],
                    "source_type": row[2],
                    "source_id": row[3],
                }
                for row in cursor.fetchall()
            ]

    finally:
        conn.close()


def mark_keywords_searched(keyword_ids: list[int]) -> int:

    unique_ids = sorted({int(keyword_id) for keyword_id in keyword_ids if keyword_id})

    if not unique_ids:
        return 0

    conn = get_connection()

    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(
                "UPDATE search_keywords SET last_searched_at = now() WHERE id = ANY(%s);",
                (unique_ids,),
            )
    finally:
        conn.close()

    logging.info("검색 키워드 last_searched_at 갱신: %d개", len(unique_ids))

    return len(unique_ids)


def save_news_search_keyword_links(items: list[dict[str, Any]]) -> dict[str, int]:

    pairs: set[tuple[int, int]] = set()

    for item in items:
        news_id = item.get("_news_id")

        if not news_id:
            continue

        for keyword_id in item.get("_search_keyword_ids", []) or []:
            if keyword_id:
                pairs.add((int(news_id), int(keyword_id)))

    if not pairs:
        logging.info("기록할 news_search_keywords 근거가 없습니다.")
        return {"linked_count": 0}

    conn = get_connection()
    linked_count = 0

    try:
        with conn, conn.cursor() as cursor:
            for news_id, keyword_id in sorted(pairs):
                cursor.execute(
                    """
                    INSERT INTO news_search_keywords (news_id, keyword_id, searched_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (news_id, keyword_id) DO NOTHING;
                    """,
                    (news_id, keyword_id),
                )

                if cursor.rowcount > 0:
                    linked_count += 1

    finally:
        conn.close()

    logging.info("news_search_keywords 근거 기록: 신규 %d건 (총 쌍 %d개)", linked_count, len(pairs))

    return {"linked_count": linked_count}


def mark_news_source_type(news_ids: list[int], source_type: str) -> int:

    unique_ids = sorted({int(news_id) for news_id in news_ids if news_id})

    if not unique_ids:
        return 0

    conn = get_connection()

    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE news
                SET source_type = %s
                WHERE id = ANY(%s)
                  AND (source_type IS NULL OR BTRIM(source_type) = '');
                """,
                (source_type, unique_ids),
            )
            updated_count = cursor.rowcount

    finally:
        conn.close()

    logging.info("news.source_type='%s' 기록: %d개", source_type, updated_count)

    return updated_count
