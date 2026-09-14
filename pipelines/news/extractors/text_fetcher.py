import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from pipelines.news.config import get_news_settings
from pipelines.news.extractors.article_metadata import extract_anchor_published_at
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)


def is_anchor_link(url: str) -> bool:
    if not url:
        return False

    anchor_host = get_news_settings().anchor_host

    if not anchor_host:
        return False

    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        return host == anchor_host or host.endswith(f".{anchor_host}")

    except Exception:
        return False


_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

_thread_local = threading.local()


def _get_session() -> requests.Session:
    """requests.Session 은 스레드 안전하지 않다. 워커 스레드마다 하나씩 두고 커넥션을 재사용한다."""

    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(_REQUEST_HEADERS)
        _thread_local.session = session
    return session


def fetch_anchor_article_data_from_url(url: str) -> tuple[str, str]:

    if not url:
        return "", ""

    if not is_anchor_link(url):
        return "", ""

    try:
        response = _get_session().get(url, timeout=10)

        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        published_at = extract_anchor_published_at(soup)

        for tag in soup(["script", "style", "nav", "footer", "aside", "iframe"]):
            tag.decompose()

        selectors = ["#dic_area", "#articeBody", "#articleBodyContents", "article"]

        for selector in selectors:
            selected = soup.select(selector)
            selector_candidates = []

            for element in selected:
                text = element.get_text(separator="\n", strip=True)

                if len(text) >= 100:
                    selector_candidates.append(text)

            if selector_candidates:
                return max(selector_candidates, key=len), published_at

        paragraphs = [p.get_text(separator="\n", strip=True) for p in soup.find_all("p")]

        paragraphs = [p for p in paragraphs if len(p) >= 30]

        fallback_text = "\n".join(paragraphs)

        if len(fallback_text) >= 100:
            return fallback_text, published_at

        return "", published_at

    except requests.exceptions.RequestException as e:
        logging.warning(f"요청 실패: {type(e).__name__}")
        return "", ""

    except Exception as e:
        logging.warning(f"추출 실패: {type(e).__name__}")
        return "", ""


def fetch_anchor_article_body_from_url(url: str) -> str:

    text, _ = fetch_anchor_article_data_from_url(url)
    return text


def fetch_article_body_from_url(url: str) -> str:

    if not url:
        return ""

    if is_anchor_link(url):
        return fetch_anchor_article_body_from_url(url)

    try:
        response = _get_session().get(url, timeout=10)

        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for tag in soup(["script", "style", "nav", "footer", "aside", "iframe", "form", "button"]):
            tag.decompose()

        selectors = [
            "[itemprop='articleBody']",
            "#dic_area",
            "#articleBody",
            "#article_body",
            "#news_body",
            "#articeBody",
            ".article_body",
            ".article-body",
            ".articleBody",
            ".news_body",
            ".news-body",
            ".newsct_article",
            ".view_text",
            ".view-content",
            ".article-view",
            ".article_cont",
            ".article-content",
            "article",
            ".content",
        ]

        for selector in selectors:
            selected = soup.select(selector)
            selector_candidates = []

            for element in selected:
                text = element.get_text(separator="\n", strip=True)

                if len(text) >= 100:
                    selector_candidates.append(text)

            if selector_candidates:
                return max(selector_candidates, key=len)

        paragraphs = [p.get_text(separator="\n", strip=True) for p in soup.find_all("p")]

        paragraphs = [p for p in paragraphs if len(p) >= 30]

        fallback_text = "\n".join(paragraphs)

        if len(fallback_text) >= 100:
            return fallback_text

        return ""

    except requests.exceptions.RequestException as e:
        logging.warning(f"요청 실패: {url} / {e}")
        return ""

    except Exception as e:
        logging.warning(f"추출 실패: {url} / {type(e).__name__}: {e}")
        return ""


def _fetch_item_body(item: dict[str, Any]) -> dict[str, Any]:
    """기사 하나의 본문을 채운다. 워커 스레드에서 실행되며 item 을 제자리 수정해 돌려준다."""

    previous_removed_noise = item.get("_body_noise_removed", [])

    if not isinstance(previous_removed_noise, list):
        previous_removed_noise = []

    removed_body_noise = []

    if "_text" in item:
        text = clean_article_body_for_storage(
            item.get("_text", ""),
            removed_noise=removed_body_noise,
            article_title=get_printable_text(item.get("title", "")),
        )
        item["_text"] = text
        item["_body_noise_removed"] = previous_removed_noise + removed_body_noise

        if removed_body_noise:
            title = get_printable_text(item.get("title", ""))
            logging.info(f"노이즈 제거: {title} / {len(removed_body_noise)}개")

        if text:
            return item

    urls = [url for url in [item.get("link", ""), item.get("originallink", "")] if url]

    text = ""
    body_source_url = ""
    published_at = ""

    for url in dict.fromkeys(urls):
        if is_anchor_link(url):
            raw_text, candidate_published_at = fetch_anchor_article_data_from_url(url)
        else:
            raw_text = fetch_article_body_from_url(url)
            candidate_published_at = ""

        if candidate_published_at and not published_at:
            published_at = candidate_published_at

        candidate_removed_noise = []
        candidate_text = clean_article_body_for_storage(
            raw_text,
            removed_noise=candidate_removed_noise,
            article_title=get_printable_text(item.get("title", "")),
        )
        removed_body_noise.extend(candidate_removed_noise)

        if candidate_text:
            text = candidate_text
            body_source_url = url
            break

    item["_text"] = text
    item["_body_source_url"] = body_source_url
    item["_body_noise_removed"] = previous_removed_noise + removed_body_noise

    if published_at and not get_printable_text(item.get("pubDate", "")):
        item["pubDate"] = published_at

    title = get_printable_text(item.get("title", ""))

    if text:
        body_source_label = "대상" if is_anchor_link(body_source_url) else body_source_url
        logging.info(
            f"추출 성공: {title} / "
            f"{len(text)}자 / source={body_source_label} / "
            f"노이즈 제거={len(removed_body_noise)}개"
        )
    else:
        logging.info(f"추출 실패: {title}")

    return item


def fetch_article_body(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """기사 본문을 스레드풀로 동시에 수집한다. 입력 순서를 그대로 유지해 돌려준다.

    기사 간 REQUEST_DELAY 대기는 걸지 않는다 — 대상이 여러 언론사 페이지라 단일 호스트
    rate limit 이 없고, 동시 요청 수(NEWS_BODY_FETCH_WORKERS)가 상한 역할을 한다.
    """

    if not items:
        return []

    workers = max(1, min(get_news_settings().news_body_fetch_workers, len(items)))

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="news-body") as executor:
        return list(executor.map(_fetch_item_body, items))
