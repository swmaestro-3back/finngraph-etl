"""두 테마가 같은 테마인지 판정한다.

배치 내 중복 병합과 기존 DB 테마 정렬(merger.py)이 같은 규칙을 쓴다.
"""

from __future__ import annotations

import re

from pipelines.themes.models import Theme

CONTAINMENT_OVERLAP_THRESHOLD = 0.9
MIN_STOCKS_FOR_CONTAINMENT = 5
MIN_CONTAINMENT_NAME_LENGTH = 2


def normalize_name(name: str) -> str:
    return re.sub(r"[\W_]+", "", name)


def overlap_coefficient(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def is_name_contained(normalized_a: str, normalized_b: str) -> bool:
    shorter, longer = sorted([normalized_a, normalized_b], key=len)
    if len(shorter) < MIN_CONTAINMENT_NAME_LENGTH:
        return False
    return shorter in longer


def find_duplicate_name(
    theme: Theme, candidate_stocks: set[str], name_to_stocks: dict[str, set[str]]
) -> str | None:
    normalized_theme_name = normalize_name(theme.name)

    for existing_name in name_to_stocks:
        if normalize_name(existing_name) == normalized_theme_name:
            return existing_name

    if len(candidate_stocks) < MIN_STOCKS_FOR_CONTAINMENT:
        return None

    for existing_name, existing_stocks in name_to_stocks.items():
        if len(existing_stocks) < MIN_STOCKS_FOR_CONTAINMENT:
            continue
        if not is_name_contained(normalized_theme_name, normalize_name(existing_name)):
            continue
        if overlap_coefficient(candidate_stocks, existing_stocks) >= CONTAINMENT_OVERLAP_THRESHOLD:
            return existing_name

    return None
