import logging
from typing import Any
from urllib.parse import urlparse


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


def remember_query_company(item: dict[str, Any], company: dict[str, Any] | None) -> None:
    """item 의 `_query_companies`(이 기사를 가져온 검색 종목, 걸린 순서)에 company 를 더한다."""

    if not company:
        return
    companies = item.setdefault("_query_companies", [])
    if all(known["company_id"] != company["company_id"] for known in companies):
        companies.append(company)


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

        # 같은 기사가 여러 종목 검색에 걸리면 먼저 걸린 종목의 기사만 남는다. 다만 이 기사를
        # 가져온 검색 종목은 모두 남은 사본에 기억한다 — 저장 여부를 그중 하나라도 통과했는지로 본다
        if matched_key:
            remember_query_company(seen_url_map[matched_key], item.get("_query_company"))
            removed_items.append(
                {
                    "removed_item": item,
                    "matched_item": seen_url_map[matched_key],
                    "reason": f"URL 중복: {matched_key}",
                    "similarity": 1.0,
                }
            )

            continue

        remember_query_company(item, item.get("_query_company"))
        unique_items.append(item)

        for url_key in url_keys:
            seen_url_map[url_key] = item

    logging.debug(f"총 {len(items)}개 중 {len(removed_items)}개 URL 중복으로 인한 드랍")

    return unique_items, removed_items
