import logging
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import requests

from pipelines.news.config import get_news_settings
from pipelines.news.repositories.search_history import CompanyQuery
from pipelines.news.utils.date_utils import parse_news_pub_date
from pipelines.news.utils.text_utils import get_printable_text


class NewsSearchError(RuntimeError):
    """네이버 검색 API 요청·응답 파싱 실패 에러"""


def validate_search_settings() -> None:
    settings = get_news_settings()

    if not settings.client_id or not settings.client_secret:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")

    if not settings.api_base_url:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")

    if settings.search_sort != "date":
        raise RuntimeError(
            f"SEARCH_SORT={settings.search_sort!r} — 워터마크 중단은 최신순(date)을 전제한다"
        )


def build_request_headers() -> dict[str, str]:
    settings = get_news_settings()

    return {
        "X-Naver-Client-Id": settings.client_id.get_secret_value(),
        "X-Naver-Client-Secret": settings.client_secret.get_secret_value(),
    }


def map_search_item_to_article(
    raw_item: dict[str, Any],
    keyword: str = "",
) -> dict[str, Any]:

    title = get_printable_text(raw_item.get("title", ""))
    description = get_printable_text(raw_item.get("description", ""))
    link = (raw_item.get("link") or "").strip()
    originallink = (raw_item.get("originallink") or "").strip() or link
    pub_date = (raw_item.get("pubDate") or "").strip()

    return {
        "title": title,
        "description": description,
        "link": link,
        "originallink": originallink,
        "pubDate": pub_date,
        "pubLabel": "news",
        "_search_keyword": keyword,
    }


def fetch_search_news_page(
    session: requests.Session,
    keyword: str,
    display: int,
    start: int,
    sort: str,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    params = {
        "query": keyword,
        "display": min(display, 100),
        "start": start,
        "sort": sort,
    }

    try:
        response = session.get(
            get_news_settings().api_base_url,
            params=params,
            headers=build_request_headers(),
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        raise NewsSearchError(f"요청 실패 (keyword={keyword}, start={start}): {e}") from e

    try:
        payload = response.json()
    except ValueError as e:
        raise NewsSearchError(f"응답 JSON 파싱 실패 (keyword={keyword}, start={start})") from e

    items = payload.get("items", [])

    if not isinstance(items, list):
        return []

    return items


def iter_search_news_pages(
    keyword: str,
    max_pages: int | None = None,
    display: int | None = None,
    sort: str | None = None,
) -> Iterator[list[dict[str, Any]]]:

    if not keyword or not keyword.strip():
        return

    settings = get_news_settings()
    max_pages = settings.search_max_pages if max_pages is None else max_pages
    display = settings.search_display if display is None else display
    sort = settings.search_sort if sort is None else sort

    display = max(1, min(display, 100))
    seen_links: set = set()
    session = requests.Session()

    try:
        for page_no in range(max_pages):
            start = 1 + page_no * display

            if start > 1000:
                break

            raw_items = fetch_search_news_page(
                session=session,
                keyword=keyword,
                display=display,
                start=start,
                sort=sort,
            )

            if not raw_items:
                break

            page_items: list[dict[str, Any]] = []

            for raw_item in raw_items:
                if not isinstance(raw_item, dict):
                    continue

                item = map_search_item_to_article(
                    raw_item=raw_item,
                    keyword=keyword,
                )

                if not item["title"] or not item["link"]:
                    continue

                if item["link"] in seen_links:
                    continue

                seen_links.add(item["link"])
                page_items.append(item)

            yield page_items

            if len(raw_items) < display:
                break

            time.sleep(settings.request_delay)

    finally:
        session.close()


def build_search_query(query: CompanyQuery) -> str:
    return get_news_settings().search_query_template.format(name=query.name)


def collection_cutoff(query: CompanyQuery, run_started_at: datetime) -> datetime:

    lookback = run_started_at - timedelta(days=get_news_settings().search_lookback_days)
    if query.watermark is None:
        return lookback
    return max(query.watermark, lookback)


def tag_query_companies(
    items: list[dict[str, Any]], query: CompanyQuery, search_keyword: str
) -> None:

    for item in items:
        item["_query_companies"] = [{"company_id": query.company_id, "name": query.name}]
        item["_search_keyword"] = search_keyword


def drop_older_than(
    items: list[dict[str, Any]], cutoff: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """pubDate 가 cutoff 보다 오래된 기사를 버린다. 파싱 실패는 기존 관례대로 남긴다."""

    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []

    for item in items:
        published_at = parse_news_pub_date(item.get("pubDate", ""))
        if published_at is not None and published_at < cutoff:
            removed.append(item)
        else:
            kept.append(item)

    return kept, removed


def collect_stock_news(query: CompanyQuery, run_started_at: datetime) -> list[dict[str, Any]]:
    """종목 하나를 최신순으로 검색

    읽다가 cutoff 보다 오래된 기사가 하나라도 나오면 그 페이지에서 멈춤
    """

    keyword = build_search_query(query)
    cutoff = collection_cutoff(query, run_started_at)
    collected: list[dict[str, Any]] = []

    for page_items in iter_search_news_pages(keyword=keyword):
        tag_query_companies(page_items, query, keyword)
        fresh, old = drop_older_than(page_items, cutoff)
        collected.extend(fresh)

        if old and page_items[-1] is old[-1]:
            break

    return collected


def collect_company_news(
    queries: list[CompanyQuery], run_started_at: datetime
) -> tuple[list[dict[str, Any]], list[int]]:
    """기업 목록을 순회 수집한다. 기업 하나의 실패는 경고 후 건너뛰고, 전부 실패하면 올린다.

    (수집 기사, 실패한 company_id 목록) 을 돌려준다 — 호출자는 실패한 기업을 마킹에서 빼서
    다음 런에 같은 창을 다시 읽게 한다.
    """

    collected: list[dict[str, Any]] = []
    failed_company_ids: list[int] = []

    for index, query in enumerate(queries):
        try:
            collected.extend(collect_stock_news(query, run_started_at))
        except NewsSearchError as e:
            failed_company_ids.append(query.company_id)
            logging.warning(
                "종목 수집 실패(건너뜀): %s (company_id=%s): %s", query.name, query.company_id, e
            )

        # 종목 사이 요청 간격 — 마지막 종목 뒤에는 쉴 필요가 없다.
        if index < len(queries) - 1:
            time.sleep(get_news_settings().request_delay)

    if queries and len(failed_company_ids) == len(queries):
        raise NewsSearchError(f"모든 종목 수집 실패 ({len(queries)}개)")

    return collected, sorted(failed_company_ids)
