import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
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

    news_text = clean_article_body_for_storage(
        item.get("_text", ""),
        article_title=get_printable_text(item.get("title", "")),
    )

    if news_text:
        return news_text[:300]

    return get_printable_text(item.get("title", ""))


def _prepare_article_body_for_storage(item: dict[str, Any]) -> str:

    removed_noise = []
    news_text = clean_article_body_for_storage(
        item.get("_text", ""),
        removed_noise=removed_noise,
        article_title=get_printable_text(item.get("title", "")),
    )
    item["_text"] = news_text

    if removed_noise:
        previous_removed_noise = item.get("_body_noise_removed", [])

        if not isinstance(previous_removed_noise, list):
            previous_removed_noise = []

        item["_body_noise_removed"] = previous_removed_noise + removed_noise

    return news_text


def has_article_body(item: dict[str, Any]) -> bool:

    return bool(
        clean_article_body_for_storage(
            item.get("_text", ""),
            article_title=get_printable_text(item.get("title", "")),
        )
    )


def insert_or_update_news(
    session, item: dict[str, Any], save_summary: bool = True, skip_existing: bool = False
) -> dict[str, Any]:

    title = get_printable_text(item.get("title", ""))
    news_text = _prepare_article_body_for_storage(item)
    summary = (build_news_summary(item) or None) if save_summary else None
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

    if not news_text:
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
            "text": news_text,
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
                    {summary_assignment}
                    text = :text,
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
                summary,
                text,
                link,
                originallink,
                published_at
            )
            VALUES (
                :title,
                :summary,
                :text,
                :link,
                :originallink,
                :published_at
            )
            ON CONFLICT (link) DO UPDATE
            SET
                title = EXCLUDED.title,
                {summary_update_sql}
                text = EXCLUDED.text,
                originallink = EXCLUDED.originallink,
                published_at = EXCLUDED.published_at
            RETURNING id;
            """
        ),
        {
            "title": title,
            "summary": summary,
            "text": news_text,
            "link": link,
            "originallink": originallink,
            "published_at": published_at,
        },
    ).fetchone()

    return {"id": inserted_row[0], "action": "inserted"}


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


def fetch_unprocessed_triple_news_items(limit: int = 100) -> list[dict[str, Any]]:
    """
    Triple ETL에서 사용.
    삼중항 추출이 아직 시도되지 않은(triple_extracted IS NULL) 뉴스를 조회한다.
    추출 중 예외가 난 뉴스는 NULL로 남아 다음 런에서 자동 재시도된다.
    """

    query = """
        SELECT
            id,
            text,
            (COALESCE(published_at, collected_at, now()))::date AS mentioned_at
        FROM news
        WHERE triple_extracted IS NULL
          AND text IS NOT NULL
          AND BTRIM(text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        return [
            {"news_id": int(news_id), "text": news_text, "mentioned_at": mentioned_at}
            for news_id, news_text, mentioned_at in rows
        ]


def mark_triple_extraction_result(
    has_triples_ids: list[int], no_triples_ids: list[int]
) -> dict[str, int]:
    """
    Triple ETL에서 호출.
    삼중항 추출을 시도한 뉴스의 triple_extracted에 삼중항 존재 여부를 마킹한다.
    (NULL=미시도이므로 TRUE/FALSE 어느 쪽이든 "시도 완료"를 겸한다)
    """

    unique_true = sorted({int(news_id) for news_id in has_triples_ids if news_id})
    unique_false = sorted({int(news_id) for news_id in no_triples_ids if news_id})

    if not unique_true and not unique_false:
        return {"true_count": 0, "false_count": 0}

    with session_scope() as session:
        if unique_true:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET triple_extracted = TRUE
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
                    SET triple_extracted = FALSE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_false},
            )

    return {"true_count": len(unique_true), "false_count": len(unique_false)}


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


def fetch_unsummarized_news_items(limit: int = 300) -> list[dict[str, Any]]:

    query = """
        SELECT
            id,
            title,
            text
        FROM news
        WHERE triple_extracted = TRUE
          AND (summary IS NULL OR BTRIM(summary) = '')
          AND text IS NOT NULL
          AND BTRIM(text) <> ''
        ORDER BY id ASC
        LIMIT :limit;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"limit": limit}).fetchall()

        items = []

        for row in rows:
            news_id, title, news_text = row

            items.append(
                {
                    "_news_id": news_id,
                    "title": title or "",
                    "_text": news_text or "",
                }
            )

        return items


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


def assign_cluster_representatives(groups: list[list[int]]) -> int:
    """클러스터 그룹별로 멤버 전원의 cluster_rep_news_id를 대표 뉴스 id로 기록한다.

    각 그룹의 첫 번째 id가 대표이며, 대표 자신도 자기 id를 가리킨다 (단독 기사 포함).
    갱신된 행 수를 반환한다.
    """

    updated_count = 0

    with session_scope() as session:
        for group in groups:
            member_ids = sorted({int(news_id) for news_id in group if news_id})

            if not member_ids:
                continue

            rep_id = int(group[0])
            result = session.execute(
                text(
                    """
                    UPDATE news
                    SET cluster_rep_news_id = :rep_id
                    WHERE id = ANY(:ids);
                    """
                ),
                {"rep_id": rep_id, "ids": member_ids},
            )
            updated_count += result.rowcount or 0

    return updated_count


def fetch_search_keywords() -> list[dict[str, Any]]:
    """search_keywords 테이블의 검색 쿼리 전체를 id 순으로 조회한다.

    keyword 한 행이 네이버 API 요청 한 번이 되며, 콤마 등 검색식은 그대로 전달된다.
    """

    query = """
        SELECT id, keyword
        FROM search_keywords
        ORDER BY id ASC;
    """

    with session_scope() as session:
        rows = session.execute(text(query)).fetchall()

        return [
            {"id": int(keyword_id), "keyword": keyword}
            for keyword_id, keyword in rows
            if keyword and keyword.strip()
        ]


def mark_keywords_searched(keyword_ids: list[int]) -> int:
    """검색을 마친 키워드들의 last_searched_at을 갱신한다. 갱신 행 수를 반환한다."""

    unique_ids = sorted({int(keyword_id) for keyword_id in keyword_ids if keyword_id})

    if not unique_ids:
        return 0

    with session_scope() as session:
        result = session.execute(
            text(
                """
                UPDATE search_keywords
                SET last_searched_at = now()
                WHERE id = ANY(:ids);
                """
            ),
            {"ids": unique_ids},
        )

        return result.rowcount or 0
