import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.filters.duplicate_filter import normalize_url_for_duplicate
from pipelines.news.utils.date_utils import parse_news_pub_date
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)

# 저장 전 같은 기사 판정: 원문 link/originallink 일치 또는 정규화 URL 일치
SELECT_EXISTING_NEWS_ID_SQL = text(
    """
    SELECT id
      FROM news
     WHERE link = :link
        OR originallink = :originallink
        OR RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:normalized_urls)
        OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:normalized_urls)
     LIMIT 1;
    """
)

# 기존 행 덮어쓰기. {summary_assignment} 는 save_summary 일 때만 "summary = :summary," 가 된다.
UPDATE_NEWS_SQL = """
    UPDATE news
       SET title = :title,
           {summary_assignment}
           text = :text,
           link = :link,
           originallink = :originallink,
           published_at = :published_at
     WHERE id = :id
    RETURNING id;
"""

# 신규 삽입. link UNIQUE 충돌 시 덮어쓴다. {summary_update} 는 save_summary 일 때만
# "summary = EXCLUDED.summary," 가 된다.
INSERT_NEWS_SQL = """
    INSERT INTO news (title, summary, text, link, originallink, published_at)
    VALUES (:title, :summary, :text, :link, :originallink, :published_at)
    ON CONFLICT (link) DO UPDATE
       SET title = EXCLUDED.title,
           {summary_update}
           text = EXCLUDED.text,
           originallink = EXCLUDED.originallink,
           published_at = EXCLUDED.published_at
    RETURNING id;
"""

# 배치의 정규화 URL 중 이미 저장된 것
SELECT_STORED_LINKS_SQL = text(
    """
    SELECT link, originallink
      FROM news
     WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:links)
        OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:links);
    """
)

# 요약 대상: 삼중항 추출을 마쳤고 요약이 아직 없는 기사
SELECT_UNSUMMARIZED_NEWS_SQL = text(
    """
    SELECT id, title, text
      FROM news
     WHERE triple_extracted = TRUE
       AND (summary IS NULL OR BTRIM(summary) = '')
       AND text IS NOT NULL
       AND BTRIM(text) <> ''
     ORDER BY id ASC
     LIMIT :limit;
    """
)

UPDATE_NEWS_SUMMARY_SQL = text(
    """
    UPDATE news
       SET summary = :summary
     WHERE id = :id;
    """
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
    published_at = parse_anchor_pub_date(item.get("pubDate", ""))

    if not title:
        raise ValueError("뉴스 제목이 비어있음")

    if not link:
        raise ValueError("뉴스 링크가 비어있음")

    if not news_text:
        raise ValueError("뉴스가 없음")

    existing_row = session.execute(
        SELECT_EXISTING_NEWS_ID_SQL,
        {
            "link": link,
            "originallink": originallink,
            "normalized_urls": [url for url in [normalized_link, normalized_originallink] if url],
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
            text(UPDATE_NEWS_SQL.format(summary_assignment=summary_assignment)),
            update_params,
        ).fetchone()

        return {
            "id": updated_row[0],
            "action": "updated",
        }

    summary_update = "summary = EXCLUDED.summary," if save_summary else ""

    inserted_row = session.execute(
        text(INSERT_NEWS_SQL.format(summary_update=summary_update)),
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
                item["_save_action"] = action

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


def remove_stored_by_url(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """link 나 originallink 가 이미 news 에 저장된 기사를 배치에서 뺀다. 남은 기사를 돌려준다."""

    normalized_links = sorted(
        {
            normalize_url_for_duplicate(url)
            for item in items
            for url in (item.get("link"), item.get("originallink"))
            if url and normalize_url_for_duplicate(url)
        }
    )

    if not normalized_links:
        return list(items)

    with session_scope() as session:
        rows = session.execute(SELECT_STORED_LINKS_SQL, {"links": normalized_links}).fetchall()

    stored_urls = {
        normalize_url_for_duplicate(url)
        for link, originallink in rows
        for url in (link, originallink)
        if url and normalize_url_for_duplicate(url)
    }

    kept = [
        item
        for item in items
        if normalize_url_for_duplicate(item.get("link")) not in stored_urls
        and normalize_url_for_duplicate(item.get("originallink")) not in stored_urls
    ]

    logging.info(f"총 {len(items)}개 중 {len(items) - len(kept)}개 DB 저장된 URL 로 드랍")

    return kept


def fetch_unsummarized_news_items(limit: int = 300) -> list[dict[str, Any]]:

    with session_scope() as session:
        rows = session.execute(SELECT_UNSUMMARIZED_NEWS_SQL, {"limit": limit}).fetchall()

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
                        UPDATE_NEWS_SUMMARY_SQL, {"summary": summary.strip(), "id": news_id}
                    )
                saved_count += 1
            except Exception as e:
                logging.warning(
                    f"요약 저장 실패(건너뜀): news_id={news_id}, error={type(e).__name__}: {e}"
                )

    logging.info(f"요약 저장 완료: {saved_count}개")

    return {"saved_count": saved_count}
