import logging
import time
from datetime import datetime
from string import Formatter
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from pipelines.news.config import (
    ANCHOR_CATEGORIES,
    ANCHOR_HOST,
    ANCHOR_URL_TEMPLATE,
    HEADLINE_MORE_COUNT,
    HEADLINE_SELECTOR,
    MORE_API_PATH_LATEST,
    MORE_API_PATH_SECTION,
    PARENT_SECTION_ID,
    REQUEST_DELAY,
)

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}


def normalize_anchor_article_link(url: str) -> str:

    if not url:
        return ""

    parsed = urlparse(url)

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def validate_anchor_headline_settings() -> None:
    if not ANCHOR_HOST:
        raise RuntimeError("ANCHOR_HOST 환경 변수가 설정되지 않았습니다.")

    if not ANCHOR_CATEGORIES:
        raise RuntimeError(
            "ANCHOR_CATEGORIES 환경 변수가 설정되지 않았습니다."
        )

    if not ANCHOR_URL_TEMPLATE:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 환경 변수가 설정되지 않았습니다."
        )

    try:
        template_fields = {
            field_name
            for _, field_name, _, _ in Formatter().parse(
                ANCHOR_URL_TEMPLATE
            )
            if field_name is not None
        }
    except ValueError as e:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 형식이 올바르지 않습니다."
        ) from e

    if "category_id" not in template_fields:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE에 {category_id}가 필요합니다."
        )

    unsupported_fields = template_fields - {"host", "category_id"}

    if unsupported_fields:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE에는 "
            "{host}와 {category_id}만 사용할 수 있습니다."
        )


def build_anchor_category_url(category_id: int) -> str:
    validate_anchor_headline_settings()

    try:
        category_url = ANCHOR_URL_TEMPLATE.format(
            host=ANCHOR_HOST,
            category_id=category_id,
        )
    except (AttributeError, IndexError, KeyError, ValueError) as e:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 형식이 올바르지 않습니다."
        ) from e

    parsed_url = urlparse(category_url)
    category_host = (parsed_url.hostname or "").lower()

    if parsed_url.scheme not in {"http", "https"} or not category_host:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE은 완전한 HTTP(S) URL이어야 합니다."
        )

    if not (
        category_host == ANCHOR_HOST
        or category_host.endswith(f".{ANCHOR_HOST}")
    ):
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE의 호스트가 "
            "ANCHOR_HOST와 일치하지 않습니다."
        )

    return category_url


def extract_section_ids(category_url: str) -> Tuple[str, str]:

    parsed = urlparse(category_url)
    numeric_segments = [
        segment
        for segment in parsed.path.split("/")
        if segment.isdigit()
    ]

    if not numeric_segments:
        return "", ""

    if len(numeric_segments) >= 2:
        return numeric_segments[0], numeric_segments[1]

    if numeric_segments[0] == PARENT_SECTION_ID:
        return PARENT_SECTION_ID, ""

    return PARENT_SECTION_ID, numeric_segments[0]


def extract_next_cursor(soup: BeautifulSoup) -> str:

    cursor_elements = soup.select("[data-cursor]")

    if not cursor_elements:
        return ""

    return (cursor_elements[-1].get("data-cursor") or "").strip()


def extract_headline_pairs(
    soup: BeautifulSoup,
    base_url: str
) -> List[tuple]:
    pairs = []

    for element in soup.select(HEADLINE_SELECTOR):
        strong = element.select_one("strong")

        if strong is not None:
            title = strong.get_text(strip=True)
        else:
            title = element.get_text(strip=True)

        href = element.get("href") or ""

        if href:
            href = urljoin(base_url, href)

        link = normalize_anchor_article_link(href)
        pairs.append((title, link))

    return pairs


def extract_rendered_html(payload: Any) -> str:

    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        rendered = payload.get("renderedComponent")

        if isinstance(rendered, dict):
            return "\n".join(
                value
                for value in rendered.values()
                if isinstance(value, str)
            )

        return "\n".join(
            extract_rendered_html(value)
            for value in payload.values()
            if isinstance(value, (str, dict, list))
        )

    if isinstance(payload, list):
        return "\n".join(
            extract_rendered_html(value)
            for value in payload
            if isinstance(value, (str, dict, list))
        )

    return ""


def fetch_more_headline_fragment(
    session: requests.Session,
    category_url: str,
    page_no: int,
    cursor: str,
    timeout: int
) -> Optional[str]:
    parsed = urlparse(category_url)
    sid, sid2 = extract_section_ids(category_url)

    api_path = MORE_API_PATH_LATEST if sid2 else MORE_API_PATH_SECTION
    api_url = f"{parsed.scheme}://{parsed.netloc}{api_path}"

    params = {
        "sid": sid,
        "sid2": sid2,
        "cluid": "",
        "pageNo": page_no,
        "date": "",
        "next": cursor,
        "_": int(time.time() * 1000),
    }

    headers = dict(REQUEST_HEADERS)
    headers["Referer"] = category_url

    try:
        response = session.get(
            api_url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logging.warning(
            "요청 실패 (sid=%s, sid2=%s, page_no=%s): %s",
            sid, sid2, page_no, e
        )
        return None

    try:
        payload = response.json()
    except ValueError:
        return response.text

    return extract_rendered_html(payload)


def iter_anchor_category_headline_pages(
    category_id: int,
    max_more_calls: int = HEADLINE_MORE_COUNT,
    wait_seconds: int = 10
) -> Iterator[List[Dict[str, Any]]]:

    category_url = build_anchor_category_url(category_id)
    category_name = ANCHOR_CATEGORIES.get(category_id, str(category_id))

    seen_links: set = set()

    def build_page_items(soup: BeautifulSoup) -> List[Dict[str, Any]]:
        page_items: List[Dict[str, Any]] = []

        for title, link in extract_headline_pairs(soup, category_url):
            if not title or not link:
                continue

            if link in seen_links:
                continue

            seen_links.add(link)

            page_items.append(
                {
                    "title": title,
                    "description": "",
                    "link": link,
                    "originallink": link,
                    "pubDate": "",
                    "pubLabel": "news",
                    "_source_type": "anchor_headline",
                    "_category_id": category_id,
                    "_category_name": category_name
                }
            )

        return page_items

    session = requests.Session()

    try:
        response = session.get(
            category_url,
            headers=REQUEST_HEADERS,
            timeout=wait_seconds,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        yield build_page_items(soup)

        cursor = extract_next_cursor(soup)
        if not cursor:
            cursor = datetime.now().strftime("%Y%m%d%H%M%S")

        for page_no in range(1, max_more_calls + 1):
            time.sleep(REQUEST_DELAY)

            fragment_html = fetch_more_headline_fragment(
                session=session,
                category_url=category_url,
                page_no=page_no,
                cursor=cursor,
                timeout=wait_seconds,
            )

            if not fragment_html:
                break

            fragment = BeautifulSoup(fragment_html, "html.parser")
            page_items = build_page_items(fragment)
            next_cursor = extract_next_cursor(fragment)

            if next_cursor:
                cursor = next_cursor

            yield page_items

            if not page_items:
                break

    finally:
        session.close()


def collect_anchor_category_headlines(
    category_id: int,
    more_click_count: int = HEADLINE_MORE_COUNT,
    wait_seconds: int = 10
) -> List[Dict[str, Any]]:

    items: List[Dict[str, Any]] = []

    for page_items in iter_anchor_category_headline_pages(
        category_id=category_id,
        max_more_calls=more_click_count,
        wait_seconds=wait_seconds,
    ):
        items.extend(page_items)

    return items


def collect_anchor_headlines(
    category_ids: List[int] | None = None,
    more_click_count: int = HEADLINE_MORE_COUNT
) -> List[Dict[str, Any]]:

    validate_anchor_headline_settings()

    category_ids = category_ids or list(ANCHOR_CATEGORIES.keys())
    all_items = []

    for category_id in category_ids:
        category_items = collect_anchor_category_headlines(
            category_id=category_id,
            more_click_count=more_click_count
        )

        all_items.extend(category_items)

    return all_items
