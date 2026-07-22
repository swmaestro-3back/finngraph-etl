import logging
from typing import Any

import psycopg2

from pipelines.news.config import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
)
from pipelines.news.transformers.duplicate_filter import (
    normalize_title_for_duplicate,
    normalize_url_for_duplicate,
)
from pipelines.news.utils.date_utils import parse_news_pub_date
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)


def get_connection():

    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def parse_anchor_pub_date(pub_date: str):

    if not pub_date:
        return None

    parsed = parse_news_pub_date(pub_date)

    if parsed is None:
        logging.warning(f"pubDate 파싱 실패: {pub_date}")

    return parsed


def build_news_summary(item: dict[str, Any]) -> str:

    explicit_summary = get_printable_text(item.get("_summary", ""))

    if explicit_summary:
        return explicit_summary

    description = get_printable_text(item.get("description", ""))
    sentiment = item.get("_sentiment", {})
    sentiment_reason = get_printable_text(sentiment.get("reason", ""))

    summary_parts = []

    if description:
        summary_parts.append(description)

    if sentiment_reason:
        summary_parts.append(f"현재 상태 판단: {sentiment_reason}")

    if summary_parts:
        return "\n".join(summary_parts)

    body_text = clean_article_body_for_storage(
        item.get("_body_text", ""),
        article_title=get_printable_text(item.get("title", "")),
    )

    if body_text:
        return body_text[:300]

    return get_printable_text(item.get("title", ""))


def _prepare_article_body_for_storage(item: dict[str, Any]) -> str:

    removed_noise = []
    body_text = clean_article_body_for_storage(
        item.get("_body_text", ""),
        removed_noise=removed_noise,
        article_title=get_printable_text(item.get("title", "")),
    )
    item["_body_text"] = body_text

    if removed_noise:
        previous_removed_noise = item.get("_body_noise_removed", [])

        if not isinstance(previous_removed_noise, list):
            previous_removed_noise = []

        item["_body_noise_removed"] = previous_removed_noise + removed_noise

    return body_text


def has_article_body(item: dict[str, Any]) -> bool:

    return bool(
        clean_article_body_for_storage(
            item.get("_body_text", ""),
            article_title=get_printable_text(item.get("title", "")),
        )
    )


def insert_or_update_news(
    cursor, item: dict[str, Any], save_summary: bool = True, skip_existing: bool = False
) -> dict[str, Any]:

    title = get_printable_text(item.get("title", ""))
    description = get_printable_text(item.get("description", ""))
    body_text = _prepare_article_body_for_storage(item)
    summary = build_news_summary(item) if save_summary else ""
    link = item.get("link", "")
    originallink = item.get("originallink", "")
    normalized_link = normalize_url_for_duplicate(link)
    normalized_originallink = normalize_url_for_duplicate(originallink)
    normalized_title = normalize_title_for_duplicate(title)
    published_at = parse_anchor_pub_date(item.get("pubDate", ""))

    if not title:
        raise ValueError("뉴스 제목이 비어있음")

    if not link:
        raise ValueError("뉴스 링크가 비어있음")

    if not body_text:
        raise ValueError("뉴스가 없음")

    cursor.execute(
        """
        SELECT id
        FROM news
        WHERE link = %s
           OR originallink = %s
           OR RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(%s)
           OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(%s)
           OR BTRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(title), '[^0-9a-z가-힣]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                )
           ) = %s
        LIMIT 1;
        """,
        (
            link,
            originallink,
            [url for url in [normalized_link, normalized_originallink] if url],
            [url for url in [normalized_link, normalized_originallink] if url],
            normalized_title,
        ),
    )
    existing_row = cursor.fetchone()

    if existing_row and skip_existing:
        return {"id": existing_row[0], "action": "skipped_existing"}

    if existing_row:
        existing_news_id = existing_row[0]
        summary_assignment = "summary = %s," if save_summary else ""
        update_values = [title, description]

        if save_summary:
            update_values.append(summary)

        update_values.extend(
            [
                body_text,
                link,
                originallink,
                published_at,
                existing_news_id,
            ]
        )

        cursor.execute(
            f"""
            UPDATE news
            SET
                title = %s,
                description = %s,
                {summary_assignment}
                body_text = %s,
                link = %s,
                originallink = %s,
                published_at = %s
            WHERE id = %s
            RETURNING id;
            """,
            tuple(update_values),
        )

        return {
            "id": cursor.fetchone()[0],
            "action": "updated",
        }

    summary_update_sql = "summary = EXCLUDED.summary," if save_summary else ""

    query = f"""
        INSERT INTO news (
            title,
            description,
            summary,
            body_text,
            link,
            originallink,
            published_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (link) DO UPDATE
        SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            {summary_update_sql}
            body_text = EXCLUDED.body_text,
            originallink = EXCLUDED.originallink,
            published_at = EXCLUDED.published_at
        RETURNING id;
    """

    cursor.execute(
        query, (title, description, summary, body_text, link, originallink, published_at)
    )

    news_id = cursor.fetchone()[0]

    return {"id": news_id, "action": "inserted"}


def save_single_news_item(
    item: dict[str, Any], save_summary: bool = True, skip_existing: bool = False
) -> int:

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                return insert_or_update_news(
                    cursor=cursor, item=item, save_summary=save_summary, skip_existing=skip_existing
                )["id"]

    finally:
        conn.close()


def save_news_items(
    items: list[dict[str, Any]], save_summary: bool = True, skip_existing: bool = False
) -> dict[str, int]:

    if not items:
        logging.info("뉴스가 없습니다.")
        return {
            "inserted_count": 0,
            "updated_count": 0,
            "skipped_existing_count": 0,
            "skipped_no_body_count": 0,
            "failed_count": 0,
        }

    conn = get_connection()

    inserted_count = 0
    updated_count = 0
    skipped_existing_count = 0
    skipped_no_body_count = 0
    failed_count = 0

    try:
        with conn:
            with conn.cursor() as cursor:
                for item in items:
                    title = get_printable_text(item.get("title", ""))

                    if not has_article_body(item):
                        skipped_no_body_count += 1
                        logging.info(f"저장 스킵: {title}")
                        continue

                    try:
                        cursor.execute("SAVEPOINT save_news_item;")

                        save_result = insert_or_update_news(
                            cursor=cursor,
                            item=item,
                            save_summary=save_summary,
                            skip_existing=skip_existing,
                        )

                        cursor.execute("RELEASE SAVEPOINT save_news_item;")

                        news_id = save_result["id"]
                        action = save_result["action"]
                        item["_news_id"] = int(news_id)

                        if action == "inserted":
                            inserted_count += 1
                            logging.info(f"뉴스 신규 저장 완료: id={news_id}, title={title}")
                        elif action == "skipped_existing":
                            skipped_existing_count += 1
                            logging.info(f"이미 DB에 있어 저장 스킵: id={news_id}, title={title}")
                        else:
                            updated_count += 1
                            logging.info(f"기존 뉴스 업데이트 완료: id={news_id}, title={title}")

                    except Exception as e:
                        try:
                            cursor.execute("ROLLBACK TO SAVEPOINT save_news_item;")
                            cursor.execute("RELEASE SAVEPOINT save_news_item;")
                        except Exception:
                            conn.rollback()

                        failed_count += 1

                        logging.error(
                            f"뉴스 저장 실패: title={title}, error={type(e).__name__}: {e}"
                        )

        logging.info(
            f"뉴스 DB 저장 완료: 신규저장 {inserted_count}개, "
            f"기존업데이트 {updated_count}개, "
            f"기존뉴스스킵 {skipped_existing_count}개, "
            f"뉴스없음스킵 {skipped_no_body_count}개, 실패 {failed_count}개"
        )

        return {
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "skipped_existing_count": skipped_existing_count,
            "skipped_no_body_count": skipped_no_body_count,
            "failed_count": failed_count,
        }

    finally:
        conn.close()


def fetch_recent_news_items(limit: int = 50) -> list[dict[str, Any]]:

    body_filter_sql = "WHERE body_text IS NOT NULL AND BTRIM(body_text) <> ''"

    query = f"""
        SELECT
            id,
            title,
            description,
            summary,
            body_text,
            link,
            originallink,
            published_at
        FROM news
        {body_filter_sql}
        ORDER BY id DESC
        LIMIT %s;
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()

                items = []

                for row in rows:
                    (
                        news_id,
                        title,
                        description,
                        summary,
                        body_text,
                        link,
                        originallink,
                        published_at,
                    ) = row

                    item = {
                        "_news_id": news_id,
                        "title": title or "",
                        "description": description or summary or "",
                        "_body_text": body_text or "",
                        "link": link or "",
                        "originallink": originallink or "",
                        "pubDate": (
                            published_at.strftime("%a, %d %b %Y %H:%M:%S %z")
                            if published_at
                            else ""
                        ),
                    }

                    items.append(item)

                return items

    finally:
        conn.close()


def fetch_unchecked_news_items(limit: int = 300) -> list[dict[str, Any]]:

    query = """
        SELECT
            id,
            title,
            description,
            summary,
            body_text,
            link,
            originallink,
            published_at
        FROM news
        WHERE material_checked_at IS NULL
          AND body_text IS NOT NULL
          AND BTRIM(body_text) <> ''
        ORDER BY id ASC
        LIMIT %s;
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()

                items = []

                for row in rows:
                    (
                        news_id,
                        title,
                        description,
                        summary,
                        body_text,
                        link,
                        originallink,
                        published_at,
                    ) = row

                    items.append(
                        {
                            "_news_id": news_id,
                            "title": title or "",
                            "description": description or summary or "",
                            "_body_text": body_text or "",
                            "link": link or "",
                            "originallink": originallink or "",
                            "pubDate": (
                                published_at.strftime("%a, %d %b %Y %H:%M:%S %z")
                                if published_at
                                else ""
                            ),
                        }
                    )

                return items

    finally:
        conn.close()


def mark_news_material_checked(kept_ids: list[int], dropped_ids: list[int]) -> dict[str, int]:

    unique_kept = sorted({int(news_id) for news_id in kept_ids if news_id})
    unique_dropped = sorted({int(news_id) for news_id in dropped_ids if news_id})

    if not unique_kept and not unique_dropped:
        logging.info("판정 결과 기록 대상이 없습니다.")
        return {"kept_count": 0, "dropped_count": 0}

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                if unique_kept:
                    cursor.execute(
                        """
                        UPDATE news
                        SET material_checked_at = now(),
                            is_material = TRUE
                        WHERE id = ANY(%s);
                        """,
                        (unique_kept,),
                    )

                if unique_dropped:
                    cursor.execute(
                        """
                        UPDATE news
                        SET material_checked_at = now(),
                            is_material = FALSE
                        WHERE id = ANY(%s);
                        """,
                        (unique_dropped,),
                    )

        result = {"kept_count": len(unique_kept), "dropped_count": len(unique_dropped)}

        logging.info(
            f"material 판정 기록 완료: 유지 {result['kept_count']}개, "
            f"소프트삭제 {result['dropped_count']}개"
        )

        return result

    finally:
        conn.close()


def find_existing_links(links: list[str]) -> set[str]:

    filtered_links = [
        normalize_url_for_duplicate(link) for link in links if normalize_url_for_duplicate(link)
    ]

    if not filtered_links:
        return set()

    query = """
        SELECT link, originallink
        FROM news
        WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(%s)
           OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(%s);
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (filtered_links, filtered_links))
                rows = cursor.fetchall()

                existing_links = set()

                for link, originallink in rows:
                    if link:
                        existing_links.add(normalize_url_for_duplicate(link))

                    if originallink:
                        existing_links.add(normalize_url_for_duplicate(originallink))

                return existing_links

    finally:
        conn.close()


def find_existing_normalized_titles(titles: list[str]) -> set[str]:

    normalized_titles = [
        normalize_title_for_duplicate(title)
        for title in titles
        if normalize_title_for_duplicate(title)
    ]

    if not normalized_titles:
        return set()

    query = """
        SELECT title
        FROM news
        WHERE BTRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(title), '[^0-9a-z가-힣]+', ' ', 'g'),
                '\\s+',
                ' ',
                'g'
            )
        ) = ANY(%s);
    """

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (normalized_titles,))
                rows = cursor.fetchall()

                return {normalize_title_for_duplicate(row[0]) for row in rows if row[0]}

    finally:
        conn.close()


def filter_new_news_by_db(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    links = []
    titles = []

    for item in items:
        links.extend([url for url in [item.get("link"), item.get("originallink")] if url])
        titles.append(item.get("title", ""))

    normalized_links = [
        normalize_url_for_duplicate(link) for link in links if normalize_url_for_duplicate(link)
    ]
    normalized_titles = [
        normalize_title_for_duplicate(title)
        for title in titles
        if normalize_title_for_duplicate(title)
    ]

    query = """
        SELECT id, link, originallink, title
        FROM news
        WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(%s)
           OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(%s)
           OR BTRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(title), '[^0-9a-z가-힣]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                )
           ) = ANY(%s);
    """

    existing_by_url = {}
    existing_by_title = {}

    if normalized_links or normalized_titles:
        conn = get_connection()

        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        query,
                        (
                            normalized_links,
                            normalized_links,
                            normalized_titles,
                        ),
                    )

                    for news_id, link, originallink, title in cursor.fetchall():
                        for url in (link, originallink):
                            normalized_url = normalize_url_for_duplicate(url)

                            if normalized_url:
                                existing_by_url[normalized_url] = int(news_id)

                        normalized_title = normalize_title_for_duplicate(title)

                        if normalized_title:
                            existing_by_title[normalized_title] = int(news_id)

        finally:
            conn.close()

    new_items = []
    existing_items = []

    for item in items:
        link = normalize_url_for_duplicate(item.get("link"))
        originallink = normalize_url_for_duplicate(item.get("originallink"))
        normalized_title = normalize_title_for_duplicate(item.get("title", ""))

        existing_news_id = existing_by_url.get(link) or existing_by_url.get(originallink)

        if existing_news_id is not None:
            item["_news_id"] = existing_news_id
            existing_items.append(
                {
                    "removed_item": item,
                    "existing_news_id": existing_news_id,
                    "reason": "DB에 이미 같은 link가 저장된 뉴스",
                }
            )
        elif normalized_title and normalized_title in existing_by_title:
            existing_news_id = existing_by_title[normalized_title]
            item["_news_id"] = existing_news_id
            existing_items.append(
                {
                    "removed_item": item,
                    "existing_news_id": existing_news_id,
                    "reason": "DB에 이미 같은 제목이 저장된 뉴스",
                }
            )
        else:
            new_items.append(item)

    return new_items, existing_items


def delete_news_by_ids(news_ids: list[int]) -> dict[str, int]:

    unique_news_ids = sorted({int(news_id) for news_id in news_ids if news_id})

    if not unique_news_ids:
        logging.info("삭제할 뉴스 id가 없습니다.")
        return {"requested_count": 0, "deleted_embedding_count": 0, "deleted_news_count": 0}

    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM news_embeddings
                    WHERE news_id = ANY(%s)
                    RETURNING news_id;
                    """,
                    (unique_news_ids,),
                )
                deleted_embeddings = cursor.fetchall()

                cursor.execute(
                    """
                    DELETE FROM news
                    WHERE id = ANY(%s)
                    RETURNING id;
                    """,
                    (unique_news_ids,),
                )
                deleted_news = cursor.fetchall()

        result = {
            "requested_count": len(unique_news_ids),
            "deleted_embedding_count": len(deleted_embeddings),
            "deleted_news_count": len(deleted_news),
        }

        logging.info(
            f"뉴스 삭제 완료: 요청={result['requested_count']}개, "
            f"임베딩삭제={result['deleted_embedding_count']}개, "
            f"뉴스삭제={result['deleted_news_count']}개"
        )

        return result

    finally:
        conn.close()


def extract_news_ids_from_removed_items(removed_items: list[dict[str, Any]]) -> list[int]:

    news_ids = []

    for removed in removed_items:
        item = removed.get("removed_item", {})
        news_id = item.get("_news_id")

        if news_id:
            news_ids.append(int(news_id))

    return news_ids
