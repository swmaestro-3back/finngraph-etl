import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from pipelines.news.config import get_news_settings
from pipelines.news.utils.text_utils import get_printable_text


def validate_search_settings() -> None:
    settings = get_news_settings()

    if not settings.client_id or not settings.client_secret:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")

    if not settings.api_base_url:
        raise RuntimeError("환경 변수가 설정되지 않았습니다.")


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
    max_pages: int | None = None,
    display: int | None = None,
    sort: str | None = None,
) -> Iterator[list[dict[str, Any]]]:

    if not keyword or not keyword.strip():
        return

    settings = get_news_settings()
    max_pages = settings.max_pages if max_pages is None else max_pages
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
