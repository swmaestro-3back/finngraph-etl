import re
from typing import Any

from pipelines.news.utils.text_utils import (
    clean_text,
    get_printable_text,
    remove_leading_title_brackets,
)

EXCLUDE_REPORT_KEYWORDS = [
    "단독",
    "기획",
    "심층",
    "르포",
    "인터뷰",
    "취재",
    "분석",
    "해설",
    "사설",
    "칼럼",
    "기자수첩",
    "탐사",
    "팩트체크",
]

EXCLUDE_TITLE_TAG_PATTERN = re.compile(r"\[\s*(포토|표)\s*\]")

EXCLUDE_FEATURED_STOCK_TAG_PATTERN = re.compile(r"\[\s*[^\]\s][^\]]*특징주[^\]]*\]")

EXCLUDE_PRICE_NOTE_PATTERN = re.compile(r"주가\s*,\s*\d{1,2}월\s*\d{1,2}일")

EXCLUDE_TITLE_PATTERNS = (
    EXCLUDE_TITLE_TAG_PATTERN,
    EXCLUDE_FEATURED_STOCK_TAG_PATTERN,
    EXCLUDE_PRICE_NOTE_PATTERN,
)


def find_excluded_title_match(title: str) -> str | None:
    """제목이 제외 패턴·키워드에 걸리면 걸린 문자열을, 아니면 None 을 돌려준다."""

    for pattern in EXCLUDE_TITLE_PATTERNS:
        if match := pattern.search(title):
            return match.group(0)

    cleaned_title = clean_text(title)
    for keyword in EXCLUDE_REPORT_KEYWORDS:
        if clean_text(keyword) in cleaned_title:
            return keyword

    return None


def filter_titles(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """제외 패턴이 하나라도 걸린 기사는 탈락시키고, 통과 기사는 제목 선두 브라켓을 제거한다.

    선두 브라켓([포토]·[…특징주])으로 거르므로 판정을 먼저 하고 제거는 통과 기사에만 한다.
    탈락 기사는 판정 근거를 남기도록 원본 제목을 유지한다.
    """

    kept_items = []
    removed_items = []

    for item in items:
        title = get_printable_text(item.get("title", ""))
        excluded_by = find_excluded_title_match(title)

        if excluded_by is None:
            item["title"] = remove_leading_title_brackets(title)
            kept_items.append(item)
        else:
            removed_items.append({"removed_item": item, "excluded_by": excluded_by})

    return kept_items, removed_items
