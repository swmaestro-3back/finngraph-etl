import time
import logging
from typing import List, Dict, Any, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from pipelines.news.config import REQUEST_DELAY, ANCHOR_HOST
from pipelines.news.collectors.article_metadata import extract_anchor_published_at
from pipelines.news.utils.text_utils import (
    clean_article_body_for_storage,
    get_printable_text,
)


DESCRIPTION_FROM_BODY_LENGTH = 150


def is_anchor_link(url: str) -> bool:
    if not url:
        return False

    if not ANCHOR_HOST:
        return False

    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        return (
            host == ANCHOR_HOST
            or host.endswith(f".{ANCHOR_HOST}")
        )

    except Exception:
        return False


def filter_only_anchor_items(
    items: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    anchor_items = []
    removed_items = []

    for item in items:
        link = item.get("link", "")

        if is_anchor_link(link):
            anchor_items.append(item)
        else:
            removed_items.append({
                "removed_item": item,
                "reason": "지정된 링크가 아님",
                "link": link,
                "originallink": item.get("originallink", "")
            })

    return anchor_items, removed_items


def fetch_anchor_article_data_from_url(url: str) -> Tuple[str, str]:

    if not url:
        return "", ""

    if not is_anchor_link(url):
        return "", ""

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        published_at = extract_anchor_published_at(soup)

        for tag in soup(["script", "style", "nav", "footer", "aside", "iframe"]):
            tag.decompose()

        selectors = [
            "#dic_area",
            "#articeBody",
            "#articleBodyContents",
            "article"
        ]

        for selector in selectors:
            selected = soup.select(selector)
            selector_candidates = []

            for element in selected:
                text = element.get_text(separator="\n", strip=True)

                if len(text) >= 100:
                    selector_candidates.append(text)

            if selector_candidates:
                return max(selector_candidates, key=len), published_at

        paragraphs = [
            p.get_text(separator="\n", strip=True)
            for p in soup.find_all("p")
        ]

        paragraphs = [
            p for p in paragraphs
            if len(p) >= 30
        ]

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

    body_text, _ = fetch_anchor_article_data_from_url(url)
    return body_text


def fetch_article_body_from_url(url: str) -> str:

    if not url:
        return ""

    if is_anchor_link(url):
        return fetch_anchor_article_body_from_url(url)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for tag in soup([
            "script",
            "style",
            "nav",
            "footer",
            "aside",
            "iframe",
            "form",
            "button"
        ]):
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

        paragraphs = [
            p.get_text(separator="\n", strip=True)
            for p in soup.find_all("p")
        ]

        paragraphs = [
            p for p in paragraphs
            if len(p) >= 30
        ]

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


def fill_empty_description_from_body(
    item: Dict[str, Any],
    body_text: str,
    max_chars: int = DESCRIPTION_FROM_BODY_LENGTH
) -> None:

    if get_printable_text(item.get("description", "")):
        return

    body_text = get_printable_text(body_text)

    if not body_text:
        return

    if len(body_text) > max_chars:
        item["description"] = body_text[:max_chars].rstrip() + "..."
    else:
        item["description"] = body_text


def enrich_items_with_article_body(
    items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    enriched_items = []

    for item in items:
        previous_removed_noise = item.get("_body_noise_removed", [])

        if not isinstance(previous_removed_noise, list):
            previous_removed_noise = []

        removed_body_noise = []

        if "_body_text" in item:
            body_text = clean_article_body_for_storage(
                item.get("_body_text", ""),
                removed_noise=removed_body_noise,
                article_title=get_printable_text(item.get("title", "")),
            )
            item["_body_text"] = body_text
            item["_body_noise_removed"] = (
                previous_removed_noise + removed_body_noise
            )
            fill_empty_description_from_body(item, body_text)

            if removed_body_noise:
                title = get_printable_text(item.get("title", ""))
                logging.info(
                    f"노이즈 제거: {title} / "
                    f"{len(removed_body_noise)}개"
                )

            if body_text:
                enriched_items.append(item)
                continue

        urls = [
            url for url in [
                item.get("link", ""),
                item.get("originallink", "")
            ]
            if url
        ]

        body_text = ""
        body_source_url = ""
        published_at = ""

        for url in dict.fromkeys(urls):
            if is_anchor_link(url):
                raw_body_text, candidate_published_at = (
                    fetch_anchor_article_data_from_url(url)
                )
            else:
                raw_body_text = fetch_article_body_from_url(url)
                candidate_published_at = ""

            if candidate_published_at and not published_at:
                published_at = candidate_published_at

            candidate_removed_noise = []
            candidate_body_text = clean_article_body_for_storage(
                raw_body_text,
                removed_noise=candidate_removed_noise,
                article_title=get_printable_text(item.get("title", "")),
            )
            removed_body_noise.extend(candidate_removed_noise)

            if candidate_body_text:
                body_text = candidate_body_text
                body_source_url = url
                break

        item["_body_text"] = body_text
        item["_body_source_url"] = body_source_url
        item["_body_noise_removed"] = (
            previous_removed_noise + removed_body_noise
        )

        if published_at and not get_printable_text(item.get("pubDate", "")):
            item["pubDate"] = published_at

        fill_empty_description_from_body(item, body_text)

        title = get_printable_text(item.get("title", ""))

        if body_text:
            body_source_label = (
                "대상" if is_anchor_link(body_source_url)
                else body_source_url
            )
            logging.info(
                f"추출 성공: {title} / "
                f"{len(body_text)}자 / source={body_source_label} / "
                f"노이즈 제거={len(removed_body_noise)}개"
            )
        else:
            logging.info(f"추출 실패: {title}")

        enriched_items.append(item)

        time.sleep(REQUEST_DELAY)

    return enriched_items
