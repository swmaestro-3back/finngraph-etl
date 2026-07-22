import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from pipelines.news.config import (
    API_BASE_URL,
    CLIENT_ID,
    CLIENT_SECRET,
    MAX_PAGES,
    REQUEST_DELAY,
    SEARCH_DISPLAY,
    SEARCH_SORT,
)
from pipelines.news.utils.text_utils import get_printable_text

SOURCE_TYPE_KEYWORD_SEARCH = "keyword_search"


def validate_search_settings() -> None:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")

    if not API_BASE_URL:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")


def build_request_headers() -> dict[str, str]:
    return {
        "X-Naver-Client-Id": CLIENT_ID or "",
        "X-Naver-Client-Secret": CLIENT_SECRET or "",
    }


def map_search_item_to_article(
    raw_item: dict[str, Any],
    keyword: str = "",
    keyword_id: int | None = None,
    source_type: str = SOURCE_TYPE_KEYWORD_SEARCH,
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
        "_source_type": source_type,
        "_search_keyword": keyword,
        "_search_keyword_ids": [keyword_id] if keyword_id else [],
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
            API_BASE_URL,
            params=params,
            headers=build_request_headers(),
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logging.warning("요청 실패 (keyword=%s, start=%s): %s", keyword, start, e)
        return []

    try:
        payload = response.json()
    except ValueError:
        logging.warning("응답 JSON 파싱 실패 (keyword=%s, start=%s)", keyword, start)
        return []

    items = payload.get("items", [])

    if not isinstance(items, list):
        return []

    return items


def iter_search_news_pages(
    keyword: str,
    keyword_id: int | None = None,
    max_pages: int = MAX_PAGES,
    display: int = SEARCH_DISPLAY,
    sort: str = SEARCH_SORT,
    source_type: str = SOURCE_TYPE_KEYWORD_SEARCH,
    wait_seconds: int = 10,
) -> Iterator[list[dict[str, Any]]]:

    if not keyword or not keyword.strip():
        return

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
                    keyword_id=keyword_id,
                    source_type=source_type,
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

            time.sleep(REQUEST_DELAY)

    finally:
        session.close()


def search_news(
    keyword: str,
    keyword_id: int | None = None,
    max_pages: int = MAX_PAGES,
    display: int = SEARCH_DISPLAY,
    sort: str = SEARCH_SORT,
    source_type: str = SOURCE_TYPE_KEYWORD_SEARCH,
) -> list[dict[str, Any]]:

    items: list[dict[str, Any]] = []

    for page_items in iter_search_news_pages(
        keyword=keyword,
        keyword_id=keyword_id,
        max_pages=max_pages,
        display=display,
        sort=sort,
        source_type=source_type,
    ):
        items.extend(page_items)

    return items
