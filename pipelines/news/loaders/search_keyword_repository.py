import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.database import session_scope


def fetch_active_search_keywords(limit: int = 50) -> list[dict[str, Any]]:

    query = """
        SELECT id, keyword, source_type, source_id
        FROM search_keywords
        WHERE status = 'active'
          AND keyword IS NOT NULL
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
                "source_type": row[2],
                "source_id": row[3],
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


def generate_theme_search_keywords() -> int:

    with session_scope() as session:
        result = session.execute(
            text(
                """
                INSERT INTO search_keywords (keyword, source_type, source_id, status)
                SELECT t.name, 'theme', t.id, 'active'
                FROM themes AS t
                WHERE t.name IS NOT NULL
                  AND BTRIM(t.name) <> ''
                ON CONFLICT (keyword) DO UPDATE
                SET source_id = EXCLUDED.source_id
                WHERE search_keywords.source_type = 'theme';
                """
            )
        )
        affected_count = result.rowcount

    logging.info("themes → search_keywords 생성/갱신: %d개", affected_count)

    return affected_count


def generate_company_search_keywords() -> int:
    """companies 마스터의 종목명에서 검색 키워드를 파생 생성한다(source_type='company').

    테마 키워드와 함께 검색어의 2축(theme + company)을 구성한다.
    이미 다른 source_type(예: theme)으로 존재하는 키워드는 건드리지 않는다.
    """

    with session_scope() as session:
        result = session.execute(
            text(
                """
                INSERT INTO search_keywords (keyword, source_type, source_id, status)
                SELECT c.name, 'company', c.id, 'active'
                FROM companies AS c
                WHERE c.name IS NOT NULL
                  AND BTRIM(c.name) <> ''
                ON CONFLICT (keyword) DO UPDATE
                SET source_id = EXCLUDED.source_id
                WHERE search_keywords.source_type = 'company';
                """
            )
        )
        affected_count = result.rowcount

    logging.info("companies → search_keywords 생성/갱신: %d개", affected_count)

    return affected_count


def save_news_theme_links(
    items: list[dict[str, Any]],
    keyword_theme_map: dict[int, int],
    link_source: str = "keyword_search",
) -> dict[str, int]:

    if not keyword_theme_map:
        logging.info("테마 연결 대상 키워드가 없습니다(source_id 미채움).")
        return {"linked_count": 0}

    pairs: set[tuple[int, int]] = set()

    for item in items:
        news_id = item.get("_news_id")

        if not news_id:
            continue

        for keyword_id in item.get("_search_keyword_ids", []) or []:
            theme_id = keyword_theme_map.get(int(keyword_id)) if keyword_id else None

            if theme_id:
                pairs.add((int(news_id), int(theme_id)))

    if not pairs:
        logging.info("기록할 news_themes 연결이 없습니다.")
        return {"linked_count": 0}

    linked_count = 0

    with session_scope() as session:
        for news_id, theme_id in sorted(pairs):
            result = session.execute(
                text(
                    """
                    INSERT INTO news_themes (news_id, theme_id, link_source, created_at)
                    VALUES (:news_id, :theme_id, :link_source, now())
                    ON CONFLICT (news_id, theme_id) DO NOTHING;
                    """
                ),
                {"news_id": news_id, "theme_id": theme_id, "link_source": link_source},
            )

            if result.rowcount > 0:
                linked_count += 1

    logging.info("news_themes 연결 기록: 신규 %d건 (총 쌍 %d개)", linked_count, len(pairs))

    return {"linked_count": linked_count}
