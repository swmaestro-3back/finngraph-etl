"""두 테마가 같은 테마인지 판정한다.

배치 내 중복 병합과 기존 DB 테마 정렬(merger.py)이 같은 규칙을 쓴다.
"""

from __future__ import annotations

import re

from pipelines.themes.models import Theme

# 괄호 없는 쪽 종목 중 이 비율 이상이 괄호 붙은 쪽에도 있어야 같은 테마로 본다
GENERIC_COVERAGE_THRESHOLD = 0.8

_PARENTHESIS = re.compile(r"\(.*?\)")


def normalize_name(name: str) -> str:
    return re.sub(r"[\W_]+", "", name).lower()


def core_name(name: str) -> str:
    """괄호 부분을 뗀 핵심 이름. "가상화폐(비트코인 등)" -> "가상화폐"."""
    return normalize_name(_PARENTHESIS.sub("", name))


def has_parenthesis(name: str) -> bool:
    return _PARENTHESIS.search(name) is not None


def overlap_coefficient(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def generic_coverage(generic_stocks: set[str], detailed_stocks: set[str]) -> float:
    """괄호 없는 쪽 종목이 괄호 붙은 쪽에 얼마나 들어 있는지.

    괄호가 부연 설명("메타버스(Metaverse)")이면 괄호 쪽이 일반 테마를 거의 다 덮지만,
    하위 분류("2차전지(전고체)")면 일반 테마보다 좁아서 이 값이 낮게 나온다.
    작은 쪽 기준인 overlap_coefficient 로는 하위 분류가 부분집합이라 둘을 구분할 수 없다.
    """
    if not generic_stocks:
        return 0.0
    return len(generic_stocks & detailed_stocks) / len(generic_stocks)


def is_parenthetical_variant(
    name_a: str, stocks_a: set[str], name_b: str, stocks_b: set[str]
) -> bool:
    # 양쪽 다 괄호면 "원자재(구리)"/"원자재(리튬)" 같은 하위 분류 나열이라 보지 않는다
    if has_parenthesis(name_a) == has_parenthesis(name_b):
        return False
    if core_name(name_a) != core_name(name_b):
        return False

    if has_parenthesis(name_a):
        generic, detailed = stocks_b, stocks_a
    else:
        generic, detailed = stocks_a, stocks_b
    return generic_coverage(generic, detailed) >= GENERIC_COVERAGE_THRESHOLD


def find_duplicate_name(
    theme: Theme, candidate_stocks: set[str], name_to_stocks: dict[str, set[str]]
) -> str | None:
    normalized_theme_name = normalize_name(theme.name)

    for existing_name in name_to_stocks:
        if normalize_name(existing_name) == normalized_theme_name:
            return existing_name

    for existing_name, existing_stocks in name_to_stocks.items():
        if is_parenthetical_variant(theme.name, candidate_stocks, existing_name, existing_stocks):
            return existing_name

    return None
