from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

_NAME_MARKERS = re.compile(r"\(주\)|㈜|\(株\)")


def parse_date(value: object) -> date | None:
    text = str(value or "").strip().replace("/", "").replace("-", "").replace(".", "")
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def parse_range(value: object) -> tuple[date | None, date | None]:
    parts = str(value or "").split("~")
    if len(parts) == 2:
        return parse_date(parts[0]), parse_date(parts[1])
    single = parse_date(value)
    return single, single


def parse_amount(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return None if number == 0 else number


def clean_name(value: object) -> str:
    return " ".join(_NAME_MARKERS.sub("", str(value or "")).split())


def clean_ticker(value: object) -> str:
    return str(value or "").strip()


def is_blank_row(row: dict[str, Any]) -> bool:
    return not any(str(value).strip() for value in row.values())


def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        if is_blank_row(row):
            continue
        key = tuple(sorted((name, str(value).strip()) for name, value in row.items()))
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result
