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

# 종목이 구분자로 이만큼 이상 이어진 제목은 여러 종목을 모은 기사(공시·시황 모음)로 본다.
# 3개는 다자 계약·협력 기사("SKT·카카오·KT, '모두의 AI' 첫선")가 많아 남긴다.
LISTING_MIN_COMPANIES = 4

# 제목 전체가 가운뎃점 나열뿐이면("A·B·C 등", "A·B·C") 이 수부터 탈락시킨다. 사전에 없는 이름이
# 섞여도 걸리도록 기업 매치가 아니라 제목의 모양으로 본다.
LIST_ONLY_MIN_ITEMS = 2

_LIST_ITEM = r"[^\s·ㆍ・]+"
LIST_ONLY_TITLE_PATTERN = re.compile(
    rf"{_LIST_ITEM}(?:\s*[·ㆍ・]\s*{_LIST_ITEM}){{{LIST_ONLY_MIN_ITEMS - 1},}}(?:\s*등)?[\s.…]*"
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


def find_listing_match(title: str, listing: int) -> str | None:
    """종목 나열 제목이면 판정 근거를, 아니면 None 을 돌려준다.

    listing 은 제목 기업 매치가 센, 구분자로 이어진 기업 수(`_title_listing`)다.
    """

    if listing >= LISTING_MIN_COMPANIES:
        return f"종목 나열 {listing}개"

    if LIST_ONLY_TITLE_PATTERN.fullmatch(remove_leading_title_brackets(title)):
        return "나열뿐인 제목"

    return None


def filter_titles(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """제외 패턴이 하나라도 걸린 기사는 탈락시키고, 통과 기사는 제목 선두 브라켓을 제거한다.

    선두 브라켓([포토]·[…특징주])으로 거르므로 판정을 먼저 하고 제거는 통과 기사에만 한다.
    탈락 기사는 판정 근거를 남기도록 원본 제목을 유지한다. 종목 나열 제목도 여기서 거른다.
    """

    kept_items = []
    removed_items = []

    for item in items:
        title = get_printable_text(item.get("title", ""))
        excluded_by = find_excluded_title_match(title) or find_listing_match(
            title, item.get("_title_listing", 0)
        )

        if excluded_by is None:
            item["title"] = remove_leading_title_brackets(title)
            kept_items.append(item)
        else:
            removed_items.append({"removed_item": item, "excluded_by": excluded_by})

    return kept_items, removed_items
