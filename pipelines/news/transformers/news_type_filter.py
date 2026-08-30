import re
from typing import Any

from pipelines.news.utils.text_utils import clean_text, get_printable_text

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
EXCLUDE_TITLE_TAG_PATTERN = re.compile(r"\[\s*(포토|표)\s*\]", re.IGNORECASE)

# "[미국 특징주]"·"[홍콩 특징주]" 같은 해외 특징주 묶음 기사 태그.
# 수식어 없는 "[특징주]"는 국내 개별 종목 뉴스라 매칭하지 않는다 — 태그 안에
# 특징주 앞의 비공백 문자를 최소 하나 요구한다.
EXCLUDE_FEATURED_STOCK_TAG_PATTERN = re.compile(r"\[\s*[^\]\s][^\]]*특징주[^\]]*\]")


def calculate_official_source_score(
    item: dict[str, Any], pipeline_input: dict[str, Any]
) -> tuple[int, dict[str, Any]]:

    title = get_printable_text(item.get("title", ""))
    cleaned_title = clean_text(title)

    debug_info = {"checked_field": "title_only", "excluded_keywords": [], "decision": ""}

    score = 0

    for match in EXCLUDE_TITLE_TAG_PATTERN.finditer(title):
        title_tag = f"[{match.group(1)}]"
        score -= 5
        debug_info["excluded_keywords"].append(
            {"keyword": title_tag, "position": "title_tag", "score": -5}
        )

    for match in EXCLUDE_FEATURED_STOCK_TAG_PATTERN.finditer(title):
        score -= 5
        debug_info["excluded_keywords"].append(
            {"keyword": match.group(0), "position": "title_tag", "score": -5}
        )

    for keyword in EXCLUDE_REPORT_KEYWORDS:
        cleaned_keyword = clean_text(keyword)

        if cleaned_keyword in cleaned_title:
            score -= 5
            debug_info["excluded_keywords"].append(
                {"keyword": keyword, "position": "title", "score": -5}
            )

    if score < 0:
        debug_info["decision"] = "excluded_by_title_keyword"
    else:
        debug_info["decision"] = "allowed_no_excluded_title_keyword"

    return score, debug_info


def filter_official_source_news(
    items: list[dict[str, Any]], pipeline_input: dict[str, Any], official_source_threshold: int = 0
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:

    official_items = []
    removed_items = []

    for item in items:
        score, debug_info = calculate_official_source_score(
            item=item, pipeline_input=pipeline_input
        )

        item["_official_source_score"] = score
        item["_official_source_debug"] = debug_info

        if score >= official_source_threshold:
            official_items.append(item)
        else:
            removed_items.append(
                {"removed_item": item, "official_source_score": score, "debug_info": debug_info}
            )

    return official_items, removed_items
