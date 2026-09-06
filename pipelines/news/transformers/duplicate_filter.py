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

            continue

        unique_items.append(item)

        for url_key in url_keys:
            seen_url_map[url_key] = item

    logging.info(f"총 {len(items)}개 중 {len(removed_items)}개 URL 중복으로 인한 드랍")

    return unique_items, removed_items


def remove_duplicate_by_title(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    unique_items = []
    removed_items = []
    seen_title_map = {}

    for item in items:
        title = get_printable_text(item.get("title", ""))
        normalized_title = normalize_title_for_duplicate(title)

        if not normalized_title:
            unique_items.append(item)
            continue

        if normalized_title in seen_title_map:
            removed_items.append(
                {
                    "removed_item": item,
                    "matched_item": seen_title_map[normalized_title],
                    "reason": f"제목 중복: {title}",
                    "similarity": 1.0,
                }
            )

            continue

        unique_items.append(item)
        seen_title_map[normalized_title] = item

    logging.info(f"총 {len(items)}개 중 {len(removed_items)}개 제목 중복으로 인한 드랍")

    return unique_items, removed_items
