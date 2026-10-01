"""
검색 대상 종목명이 제목·스니펫에 없는 기사를 거른다
"""

from __future__ import annotations

from typing import Any


def mentions_query_company(item: dict[str, Any]) -> bool:
    """검색 대상 종목명(쿼리 템플릿에 들어간 이름)이 제목·스니펫에 있는지"""

    company = item.get("_query_company")
    if not company:
        return False
    text = f"{item.get('title', '')} {item.get('description', '')}".casefold()
    return company["name"].casefold() in text


def drop_query_company_unmentioned(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """검색 대상 종목명이 제목·스니펫에 없는 기사를 버린다. (남은 기사, 버린 기사) 를 돌려준다."""

    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    for item in items:
        (kept if mentions_query_company(item) else removed).append(item)
    return kept, removed
