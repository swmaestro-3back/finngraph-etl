import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.news.transformers.duplicate_filter import (
    normalize_title_for_duplicate,
    normalize_url_for_duplicate,
)
from pipelines.news.utils.date_utils import parse_news_pub_date
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
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
    session, item: dict[str, Any], save_summary: bool = True, skip_existing: bool = False
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

    existing_row = session.execute(
        text(
            """
            SELECT id
            FROM news
            WHERE link = :link
               OR originallink = :originallink
               OR RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:normalized_urls)
               OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:normalized_urls)
               OR BTRIM(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(LOWER(title), '[^0-9a-z가-힣]+', ' ', 'g'),
                        '\\s+',
                        ' ',
                        'g'
                    )
               ) = :normalized_title
            LIMIT 1;
            """
        ),
        {
            "link": link,
            "originallink": originallink,
            "normalized_urls": [url for url in [normalized_link, normalized_originallink] if url],
            "normalized_title": normalized_title,
        },
    ).fetchone()

    if existing_row and skip_existing:
        return {"id": existing_row[0], "action": "skipped_existing"}

    if existing_row:
        existing_news_id = existing_row[0]

        update_params = {
            "title": title,
            "description": description,
            "body_text": body_text,
            "link": link,
            "originallink": originallink,
            "published_at": published_at,
            "id": existing_news_id,
        }

        summary_assignment = ""

        if save_summary:
            summary_assignment = "summary = :summary,"
            update_params["summary"] = summary

        updated_row = session.execute(
            text(
                f"""
                UPDATE news
                SET
                    title = :title,
                    description = :description,
                    {summary_assignment}
                    body_text = :body_text,
                    link = :link,
                    originallink = :originallink,
                    published_at = :published_at
                WHERE id = :id
                RETURNING id;
                """
            ),
            update_params,
        ).fetchone()

        return {
            "id": updated_row[0],
            "action": "updated",
        }

    summary_update_sql = "summary = EXCLUDED.summary," if save_summary else ""

    inserted_row = session.execute(
        text(
            f"""
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
                :title,
                :description,
                :summary,
                :body_text,
                :link,
                :originallink,
                :published_at
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
        ),
        {
            "title": title,
            "description": description,
            "summary": summary,
            "body_text": body_text,
            "link": link,
            "originallink": originallink,
            "published_at": published_at,
        },
    ).fetchone()

    return {"id": inserted_row[0], "action": "inserted"}


def save_single_news_item(
    item: dict[str, Any], save_summary: bool = True, skip_existing: bool = False
) -> int:

    with session_scope() as session:
        return insert_or_update_news(
            session=session, item=item, save_summary=save_summary, skip_existing=skip_existing
        )["id"]


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

    inserted_count = 0
    updated_count = 0
    skipped_existing_count = 0
    skipped_no_body_count = 0
    failed_count = 0

    with session_scope() as session:
        for item in items:
            title = get_printable_text(item.get("title", ""))

            if not has_article_body(item):
                skipped_no_body_count += 1
                logging.info(f"저장 스킵: {title}")
                continue

            try:
                # 항목마다 SAVEPOINT(begin_nested)로 격리해 개별 실패가 전체를 깨지 않게 한다.
                with session.begin_nested():
                    save_result = insert_or_update_news(
                        session=session,
                        item=item,
                        save_summary=save_summary,
                        skip_existing=skip_existing,
                    )

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
                failed_count += 1

                logging.error(f"뉴스 저장 실패: title={title}, error={type(e).__name__}: {e}")

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
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

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
                    published_at.strftime("%a, %d %b %Y %H:%M:%S %z") if published_at else ""
                ),
            }

            items.append(item)

        return items


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
        WHERE is_material IS NULL
          AND body_text IS NOT NULL
          AND BTRIM(body_text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

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
                        published_at.strftime("%a, %d %b %Y %H:%M:%S %z") if published_at else ""
                    ),
                }
            )

        return items


def mark_news_material_checked(kept_ids: list[int], dropped_ids: list[int]) -> dict[str, int]:

    unique_kept = sorted({int(news_id) for news_id in kept_ids if news_id})
    unique_dropped = sorted({int(news_id) for news_id in dropped_ids if news_id})

    if not unique_kept and not unique_dropped:
        logging.info("판정 결과 기록 대상이 없습니다.")
        return {"kept_count": 0, "dropped_count": 0}

    with session_scope() as session:
        if unique_kept:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET is_material = TRUE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_kept},
            )

        if unique_dropped:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET is_material = FALSE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_dropped},
            )

    result = {"kept_count": len(unique_kept), "dropped_count": len(unique_dropped)}

    logging.info(
        f"material 판정 기록 완료: 유지 {result['kept_count']}개, "
        f"소프트삭제 {result['dropped_count']}개"
    )

    return result


def fetch_unprocessed_triplet_news_items(limit: int = 100) -> list[dict[str, Any]]:
    """
    Triplet ETL에서 사용
    삼중항관계 추출이 아직 진행되지 않아, relation_extracted값이 NULL인 뉴스들을 조회

    필터링되어 유효한 뉴스라고 판별난 is_material=True
    + 삼중항추출 안된 것 relation_extracted IS NULL
    """

    query = """
        SELECT
            id,
            body_text
        FROM news
        WHERE relation_extracted IS NULL
          AND is_material = TRUE
          AND body_text IS NOT NULL
          AND BTRIM(body_text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        return [{"news_id": int(news_id), "body_text": body_text} for news_id, body_text in rows]


def mark_news_relation_extracted(
    has_triplets_ids: list[int], no_triplets_ids: list[int]
) -> dict[str, int]:
    """
    Triplet ETL에서 호출
    삼중항관계 추출 여부에 따른 BOOLEAN값을 news 테이블의 relation_extracted에 마킹하기 위한 함수
    """

    unique_true = sorted({int(news_id) for news_id in has_triplets_ids if news_id})
    unique_false = sorted({int(news_id) for news_id in no_triplets_ids if news_id})

    if not unique_true and not unique_false:
        return {"true_count": 0, "false_count": 0}

    with session_scope() as session:
        if unique_true:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET relation_extracted = TRUE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_true},
            )

        if unique_false:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET relation_extracted = FALSE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_false},
            )

    return {"true_count": len(unique_true), "false_count": len(unique_false)}


def find_existing_links(links: list[str]) -> set[str]:

    filtered_links = [
        normalize_url_for_duplicate(link) for link in links if normalize_url_for_duplicate(link)
    ]

    if not filtered_links:
        return set()

    query = """
        SELECT link, originallink
        FROM news
        WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:links)
           OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:links);
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"links": filtered_links}).fetchall()

        existing_links = set()

        for link, originallink in rows:
            if link:
                existing_links.add(normalize_url_for_duplicate(link))

            if originallink:
                existing_links.add(normalize_url_for_duplicate(originallink))

        return existing_links


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
        ) = ANY(:titles);
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"titles": normalized_titles}).fetchall()

        return {normalize_title_for_duplicate(row[0]) for row in rows if row[0]}


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
        WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:links)
           OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:links)
           OR BTRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(LOWER(title), '[^0-9a-z가-힣]+', ' ', 'g'),
                    '\\s+',
                    ' ',
                    'g'
                )
           ) = ANY(:titles);
    """

    existing_by_url = {}
    existing_by_title = {}

    if normalized_links or normalized_titles:
        with session_scope() as session:
            rows = session.execute(
                text(query),
                {
                    "links": normalized_links,
                    "titles": normalized_titles,
                },
            ).fetchall()

            for news_id, link, originallink, title in rows:
                for url in (link, originallink):
                    normalized_url = normalize_url_for_duplicate(url)

                    if normalized_url:
                        existing_by_url[normalized_url] = int(news_id)

                normalized_title = normalize_title_for_duplicate(title)

                if normalized_title:
                    existing_by_title[normalized_title] = int(news_id)

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
    """뉴스 하드 삭제. 자식 테이블(news_relations, news_themes 등)은 FK CASCADE로 함께 정리된다."""

    unique_news_ids = sorted({int(news_id) for news_id in news_ids if news_id})

    if not unique_news_ids:
        logging.info("삭제할 뉴스 id가 없습니다.")
        return {"requested_count": 0, "deleted_news_count": 0}

    with session_scope() as session:
        deleted_news = session.execute(
            text(
                """
                DELETE FROM news
                WHERE id = ANY(:ids)
                RETURNING id;
                """
            ),
            {"ids": unique_news_ids},
        ).fetchall()

    result = {
        "requested_count": len(unique_news_ids),
        "deleted_news_count": len(deleted_news),
    }

    logging.info(
        f"뉴스 삭제 완료: 요청={result['requested_count']}개, "
        f"뉴스삭제={result['deleted_news_count']}개"
    )

    return result


def extract_news_ids_from_removed_items(removed_items: list[dict[str, Any]]) -> list[int]:

    news_ids = []

    for removed in removed_items:
        item = removed.get("removed_item", {})
        news_id = item.get("_news_id")

        if news_id:
            news_ids.append(int(news_id))

    return news_ids


def fetch_unsummarized_news_items(limit: int = 300) -> list[dict[str, Any]]:

    query = """
        SELECT
            id,
            title,
            body_text
        FROM news
        WHERE relation_extracted IS NOT NULL
          AND (summary IS NULL OR BTRIM(summary) = '')
          AND body_text IS NOT NULL
          AND BTRIM(body_text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        items = []

        for row in rows:
            news_id, title, body_text = row

            items.append(
                {
                    "_news_id": news_id,
                    "title": title or "",
                    "_body_text": body_text or "",
                }
            )

        return items


def fetch_triplets_for_news_ids(
    news_ids: list[int],
) -> dict[int, list[tuple[str, str, str]]]:
    """뉴스 id별 삼중항 `(subject_name, relation, object_name)` 목록을 조회한다."""

    unique_ids = sorted({int(news_id) for news_id in news_ids if news_id})

    if not unique_ids:
        return {}

    query = """
        SELECT news_id, subject_name, relation, object_name
        FROM news_relations
        WHERE news_id = ANY(:ids)
        ORDER BY news_id ASC, id ASC;
    """

    triplets_by_news: dict[int, list[tuple[str, str, str]]] = {}

    with session_scope() as session:
        rows = session.execute(text(query), {"ids": unique_ids}).fetchall()

    for news_id, subject_name, relation, object_name in rows:
        triplets_by_news.setdefault(int(news_id), []).append((subject_name, relation, object_name))

    return triplets_by_news


def save_news_summaries(rows: list[tuple[int, str]]) -> dict[str, int]:

    normalized = [
        (int(news_id), summary)
        for news_id, summary in rows
        if news_id and summary and summary.strip()
    ]

    if not normalized:
        logging.info("저장할 요약이 없습니다.")
        return {"saved_count": 0}

    saved_count = 0

    with session_scope() as session:
        for news_id, summary in normalized:
            try:
                with session.begin_nested():
                    session.execute(
                        text(
                            """
                            UPDATE news
                            SET summary = :summary
                            WHERE id = :id;
                            """
                        ),
                        {"summary": summary.strip(), "id": news_id},
                    )
                saved_count += 1
            except Exception as e:
                logging.warning(
                    f"요약 저장 실패(건너뜀): news_id={news_id}, error={type(e).__name__}: {e}"
                )

    logging.info(f"요약 저장 완료: {saved_count}개")

    return {"saved_count": saved_count}
