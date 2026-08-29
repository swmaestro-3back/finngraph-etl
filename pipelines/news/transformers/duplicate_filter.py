import logging
import re
from typing import Any
from urllib.parse import urlparse

from pipelines.news.utils.text_utils import get_printable_text


def normalize_title_for_duplicate(title: str) -> str:

    title = get_printable_text(title)
    title = title.lower()
    title = re.sub(r"[^0-9a-z가-힣]+", " ", title)

    return " ".join(title.split()).lower()


def normalize_url_for_duplicate(url: str) -> str:

    if not url:
        return ""

    try:
        parsed = urlparse(url.strip())

        if not parsed.scheme or not parsed.netloc:
            return url.strip()

        path = parsed.path.rstrip("/")

        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"

    except Exception:
        return url.strip()


def remove_duplicate_by_url(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    unique_items = []
    removed_items = []
    seen_url_map = {}

    for item in items:
        originallink = item.get("originallink", "")
        link = item.get("link", "")

        url_keys = [
            normalized_url
            for normalized_url in [
                normalize_url_for_duplicate(originallink),
                normalize_url_for_duplicate(link),
            ]
            if normalized_url
        ]

        matched_key = next((url_key for url_key in url_keys if url_key in seen_url_map), None)

        if matched_key:
            removed_items.append(
                {
                    "removed_item": item,
                    "matched_item": seen_url_map[matched_key],
                    "reason": f"URL 중복: {matched_key}",
                    "similarity": 1.0,
                }
            )

            title = get_printable_text(item.get("title", ""))
            logging.info(f"URL 중복 제거: {title}")
            continue

        unique_items.append(item)

        for url_key in url_keys:
            seen_url_map[url_key] = item

    return unique_items, removed_items
