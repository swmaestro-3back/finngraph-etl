import json
import logging
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.news_companies import (
    link_news_companies,
    linked_company_ids,
)
from pipelines.news.transformers.clustering.document import TermWeights
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

# 요약 대상: 승격된 클러스터의 대표 기사이고 핵심 포인트가 아직 없는 것. 삼중항 추출 결과를 기다리지
# 않는다. 요약 문단과 포인트는 한 번에 저장되므로 summary_points 만 본다 — 포인트 없이 문단만 있는
# 옛 요약도 대상이라 새 형식으로 다시 만들어진다. member_count 조건은 옛 로직이 만든 작은
# 클러스터(대표는 있지만 후보 수가 승격 기준 미만)를 뺀다.
SELECT_UNSUMMARIZED_NEWS_SQL = text(
    """
    SELECT n.id, n.title, n.text, n.published_at
      FROM news n
      JOIN news_clusters nc ON nc.representative_news_id = n.id
     WHERE nc.member_count >= :promote_size
       AND n.summary_points IS NULL
       AND n.text IS NOT NULL
       AND BTRIM(n.text) <> ''
     ORDER BY n.id ASC;
    """
)

# 사건이 없는 기사는 포인트가 빈 배열([])이다 — NULL 이 아니라서 다시 요약 대상이 되지 않는다.
UPDATE_NEWS_SUMMARY_SQL = text(
    """
    UPDATE news
       SET summary = :summary,
           summary_points = CAST(:points AS jsonb)
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


def save_news_summaries(rows: list[tuple[int, str, list[dict[str, str]]]]) -> dict[str, int]:
    """(news_id, 요약 문단, 핵심 포인트) 를 저장한다. 문단이 빈 행은 건너뛴다."""

    normalized = [
        (int(news_id), summary.strip(), points or [])
        for news_id, summary, points in rows
        if news_id and summary and summary.strip()
    ]

    if not normalized:
        logging.debug("저장할 요약이 없습니다.")
        return {"saved_count": 0}

    saved_count = 0

    with session_scope() as session:
        for news_id, summary, points in normalized:
            try:
                with session.begin_nested():
                    session.execute(
                        UPDATE_NEWS_SUMMARY_SQL,
                        {
                            "summary": summary,
                            "points": json.dumps(points, ensure_ascii=False),
                            "id": news_id,
                        },
                    )
                saved_count += 1
            except Exception as e:
                logging.warning(
                    f"요약 저장 실패(건너뜀): news_id={news_id}, error={type(e).__name__}: {e}"
                )

    logging.debug(f"요약 저장 완료: {saved_count}개")

    return {"saved_count": saved_count}


# ── 클러스터링 입력 ──────────────────────────────────────────────────────────
# 벡터화 대상: term_weights 없는 저장 기사 전량. 본문이 없는 행은 옛 구조에서 남은 것이고 판정하지
# 않으므로 벡터화도 하지 않는다. 기업은 news_companies 에서 모아 entities 서브벡터가 된다.
SELECT_UNVECTORIZED_NEWS_SQL = text(
    """
    SELECT n.id, n.title, n.text,
           COALESCE(ARRAY_AGG(nc.company_id ORDER BY nc.company_id)
                    FILTER (WHERE nc.company_id IS NOT NULL), '{}') AS company_ids
      FROM news n
      LEFT JOIN news_companies nc ON nc.news_id = n.id
     WHERE n.term_weights IS NULL
       AND n.text IS NOT NULL
       AND BTRIM(n.text) <> ''
     GROUP BY n.id
     ORDER BY n.id ASC;
    """
)

UPDATE_TERM_WEIGHTS_SQL = text(
    "UPDATE news SET term_weights = CAST(:terms AS jsonb) WHERE id = :news_id;"
)

# 판정 대상: 벡터화는 됐고 클러스터는 없는 기사 전량(발행 시각순). 발행 시각이 없으면 수집 시각으로
# 본다.
SELECT_UNCLUSTERED_NEWS_SQL = text(
    """
    SELECT id, title, term_weights, COALESCE(published_at, collected_at, now()) AS published_at
      FROM news
     WHERE cluster_id IS NULL
       AND term_weights IS NOT NULL
     ORDER BY 4 ASC, id ASC;
    """
)

# 백필 DAG 가 하류를 깨울지 정하는 수. 벡터화 전 기사도 본문만 있으면 곧 판정되므로 센다 — 백필 수집
# 직후에는 아무 기사도 벡터화되지 않았다. 본문 없는 옛 행은 벡터화·판정 모두 하지 않으므로 뺀다.
COUNT_UNCLUSTERED_NEWS_SQL = text(
    "SELECT COUNT(*) FROM news WHERE cluster_id IS NULL AND text IS NOT NULL AND BTRIM(text) <> '';"
)

# 클러스터별 멤버 기사(발행 시각순). 대표 선정과 제목 생성의 입력이다. 본문은 길이만 읽는다.
SELECT_CLUSTER_MEMBER_ARTICLES_SQL = text(
    """
    SELECT cluster_id, id, title, term_weights, COALESCE(length(text), 0) AS body_length,
           COALESCE(published_at, collected_at) AS published_at
      FROM news
     WHERE cluster_id = ANY(:cluster_ids)
     ORDER BY cluster_id, 6 ASC NULLS LAST, id ASC;
    """
)


def fetch_unvectorized_news() -> list[dict[str, Any]]:
    """벡터화 대상 전량(id 순). company_ids 는 news_companies 의 기업이다(없으면 빈 목록)."""

    with session_scope() as session:
        rows = session.execute(SELECT_UNVECTORIZED_NEWS_SQL).fetchall()

    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "text": body or "",
            "company_ids": [int(company_id) for company_id in company_ids or []],
        }
        for news_id, title, body, company_ids in rows
    ]


def save_term_weights(rows: list[tuple[int, TermWeights]]) -> dict[str, int]:
    """(news_id, term_weights) 를 저장한다. 기사마다 SAVEPOINT 라 실패는 그 기사만 되돌린다."""

    counts = {"saved": 0, "failed": 0}
    if not rows:
        return counts

    with session_scope() as session:
        for news_id, term_weights in rows:
            try:
                with session.begin_nested():
                    session.execute(
                        UPDATE_TERM_WEIGHTS_SQL,
                        {
                            "news_id": int(news_id),
                            "terms": json.dumps(term_weights, ensure_ascii=False),
                        },
                    )
            except Exception as e:
                counts["failed"] += 1
                logging.error(
                    f"벡터화 기록 실패(건너뜀): news_id={news_id}, error={type(e).__name__}: {e}"
                )
                continue
            counts["saved"] += 1

    return counts


def fetch_unclustered_news() -> list[dict[str, Any]]:
    """클러스터 판정 대상 전량(발행 시각순). 기록에 실패해 cluster_id 가 없는 기사도 다시 든다."""

    with session_scope() as session:
        rows = session.execute(SELECT_UNCLUSTERED_NEWS_SQL).fetchall()

    return [
        {
            "_news_id": int(news_id),
            "title": title or "",
            "term_weights": term_weights or {},
            "published_at": published_at,
        }
        for news_id, title, term_weights, published_at in rows
    ]


def count_unclustered_news() -> int:
    """클러스터가 없고 본문이 있는 기사 수(벡터화 전 포함). 백필 DAG 의 하류 발행 판단에 쓴다."""

    with session_scope() as session:
        return int(session.execute(COUNT_UNCLUSTERED_NEWS_SQL).scalar_one())


def fetch_cluster_member_articles(cluster_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """클러스터별 멤버 기사(발행 시각순). 멤버가 없는 클러스터는 키가 없다."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_MEMBER_ARTICLES_SQL, {"cluster_ids": [int(c) for c in cluster_ids]}
        ).fetchall()

    members: dict[int, list[dict[str, Any]]] = {}
    for cluster_id, news_id, title, term_weights, body_length, published_at in rows:
        members.setdefault(int(cluster_id), []).append(
            {
                "_news_id": int(news_id),
                "title": title or "",
                "term_weights": term_weights or {},
                "body_length": int(body_length or 0),
                "published_at": published_at,
            }
        )
    return members
