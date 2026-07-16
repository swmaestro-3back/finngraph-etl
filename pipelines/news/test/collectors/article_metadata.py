from typing import Any


def extract_anchor_published_at(soup: Any) -> str:

    selectors_and_attributes = [
        ("meta[property='article:published_time']", "content"),
        ("meta[name='article:published_time']", "content"),
        ("span._ARTICLE_DATE_TIME", "data-date-time"),
        ("span.media_end_head_info_datestamp_time", "data-date-time"),
    ]

    for selector, attribute in selectors_and_attributes:
        element = soup.select_one(selector)

        if not element:
            continue

        published_at = str(element.get(attribute, "")).strip()

        if published_at:
            return published_at

    return ""
