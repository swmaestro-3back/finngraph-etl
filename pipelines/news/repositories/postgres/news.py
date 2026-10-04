import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_companies import (
    link_news_companies,
    linked_company_ids,
)
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


# 본문과 함께 신규 삽입. link UNIQUE 충돌이면 아무것도 돌려주지 않는다.
INSERT_NEWS_SQL = text(
    """
    INSERT INTO news (title, text, link, originallink, published_at)
    VALUES (:title, :text, :link, :originallink, :published_at)
    ON CONFLICT (link) DO NOTHING
    RETURNING id;
    """
)

# 배치의 정규화 URL 중 이미 저장된 것
SELECT_STORED_LINKS_SQL = text(
    """
    SELECT link, originallink
      FROM news
     WHERE RTRIM(LOWER(SPLIT_PART(link, '?', 1)), '/') = ANY(:links)
        OR RTRIM(LOWER(SPLIT_PART(originallink, '?', 1)), '/') = ANY(:links);
    """
)

# 요약 대상: 승격된 클러스터의 대표 기사이고 요약이 아직 없는 것. 삼중항 추출 결과를 기다리지
# 않는다. member_count 조건은 옛 로직이 만든 작은 클러스터(대표는 있지만 후보 수가 승격 기준
# 미만)를 뺀다.
SELECT_UNSUMMARIZED_NEWS_SQL = text(
    """
    SELECT n.id, n.title, n.text, n.published_at
      FROM news n
      JOIN news_clusters nc ON nc.representative_news_id = n.id
     WHERE nc.member_count >= :promote_size
       AND (n.summary IS NULL OR BTRIM(n.summary) = '')
       AND n.text IS NOT NULL
       AND BTRIM(n.text) <> ''
     ORDER BY n.id ASC;
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


def insert_news(session, item: dict[str, Any]) -> dict[str, Any]:
    """본문과 함께 기사 한 건을 넣는다. 같은 기사가 이미 있으면 그 id 로 skipped_existing 이다."""

    title = get_printable_text(item.get("title", ""))
    link = item.get("link", "")
    originallink = item.get("originallink", "")
    normalized_urls = [
        url
        for url in (normalize_url_for_duplicate(link), normalize_url_for_duplicate(originallink))
        if url
    ]

    if not title:
        raise ValueError("뉴스 제목이 비어있음")

    if not link:
        raise ValueError("뉴스 링크가 비어있음")

    news_text = _prepare_article_body_for_storage(item)
    if not news_text:
        raise ValueError("뉴스 본문이 비어있음")

    existing_params = {
        "link": link,
        "originallink": originallink,
        "normalized_urls": normalized_urls,
    }
    existing_row = session.execute(SELECT_EXISTING_NEWS_ID_SQL, existing_params).fetchone()

    if existing_row:
        return {"id": existing_row[0], "action": "skipped_existing"}

    inserted_row = session.execute(
        INSERT_NEWS_SQL,
        {
            "title": title,
            "text": news_text,
            "link": link,
            "originallink": originallink,
            "published_at": parse_anchor_pub_date(item.get("pubDate", "")),
        },
    ).fetchone()

    if inserted_row is None:
        # 조회와 삽입 사이에 같은 link 가 들어왔다
        existing_row = session.execute(SELECT_EXISTING_NEWS_ID_SQL, existing_params).fetchone()
        return {"id": existing_row[0], "action": "skipped_existing"}

    return {"id": inserted_row[0], "action": "inserted"}


def save_news(items: list[dict[str, Any]]) -> dict[str, int]:
    """본문과 기업 판정까지 끝난 기사를 기업 연결과 함께 저장하고 `_news_id`·`_save_action` 을
    붙인다.

    기사 INSERT 와 news_companies INSERT 가 같은 SAVEPOINT 안이다 — 함께 저장되거나 함께 버려진다.
    news_companies 는 Event 당사자와 삼중항 엔티티의 유일한 원천이라, 연결 없는 기사가 남으면
    그 기사는 하류에서 쓸 수 없다. 이미 있던 기사(skipped_existing)는 첫 저장 때 연결이 끝났으므로
    다시 연결하지 않는다.

    본문이 없는 기사는 저장하지 않는다(실패로 센다) — 수집 잡이 미리 걸러서 보내므로 여기서
    걸리면 정제 결과가 달라진 경우다.
    """

    counts = {
        "inserted_count": 0,
        "skipped_existing_count": 0,
        "failed_count": 0,
        "linked_count": 0,
    }

    if not items:
        return counts

    with session_scope() as session:
        for item in items:
            try:
                # 항목마다 SAVEPOINT(begin_nested)로 격리해 개별 실패가 전체를 깨지 않게 한다.
                with session.begin_nested():
                    save_result = insert_news(session, item)
                    linked = 0
                    if save_result["action"] == "inserted":
                        linked = link_news_companies(
                            session, int(save_result["id"]), linked_company_ids(item)
                        )
            except Exception as e:
                counts["failed_count"] += 1
                logging.error(f"뉴스 저장 실패: {type(e).__name__}: {e}")
                continue

            item["_news_id"] = int(save_result["id"])
            item["_save_action"] = save_result["action"]
            counts[f"{save_result['action']}_count"] += 1
            counts["linked_count"] += linked

    logging.debug(
        f"뉴스 저장 완료: 신규 {counts['inserted_count']}개, "
        f"기존 스킵 {counts['skipped_existing_count']}개, 실패 {counts['failed_count']}개"
    )

    return counts


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

    logging.debug(f"총 {len(items)}개 중 {len(items) - len(kept)}개 DB 저장된 URL 로 드랍")

    return kept


def fetch_unsummarized_news_items(promote_size: int) -> list[dict[str, Any]]:
    """요약 대상 전량. 런마다 상한 없이 밀린 기사를 모두 처리한다.

    후보가 promote_size 건 이상인 클러스터의 대표만 대상이다.
    """

    with session_scope() as session:
        rows = session.execute(
            SELECT_UNSUMMARIZED_NEWS_SQL, {"promote_size": promote_size}
        ).fetchall()

        items = []

        for row in rows:
            news_id, title, news_text, published_at = row

            items.append(
                {
                    "_news_id": news_id,
                    "title": title or "",
                    "_text": news_text or "",
                    "_published_at": published_at,
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
        logging.debug("저장할 요약이 없습니다.")
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

    logging.debug(f"요약 저장 완료: {saved_count}개")

    return {"saved_count": saved_count}
