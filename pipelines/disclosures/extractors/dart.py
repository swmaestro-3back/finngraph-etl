"""OpenDART 공시 목록·원문 수집.

단일판매ㆍ공급계약체결에는 전용 API가 없다. 거래소공시(pblntf_ty='I') 목록을 받아
보고서명으로 거른다. 목록은 100건/page 라 total_page 까지 직접 순회한다.

일 호출 한도(020/021)는 다른 오류와 구분해 QuotaExceeded 로 올린다 — job 이 이를 받으면
실패가 아니라 "여기까지 하고 다음 실행에서 이어감"으로 처리해야 하기 때문이다(재시도는
소진된 한도에 다시 부딪힐 뿐이다).
"""

from __future__ import annotations

import calendar
import re
from datetime import date, timedelta
from typing import Any

from pipelines.common.clients.dart import (
    NO_DATA_STATUSES,
    QUOTA_STATUSES,
    DartApiError,
    DartClient,
    QuotaExceeded,
)

DISCLOSURE_LIST_PATH = "list.json"

# 거래소공시. 단일판매ㆍ공급계약체결은 이 유형으로 접수된다.
DISCLOSURE_KIND = "I"
TARGET_REPORT_NM = "단일판매ㆍ공급계약체결"
LIST_PAGE_COUNT = 100

_PREFIX_TAG = re.compile(r"^\[[^\]]*\]")


def is_target_report(report_nm: str | None) -> bool:
    """정정 접두([기재정정] 등)를 뗀 뒤 보고서명이 정확히 일치하는지.

    '단일판매ㆍ공급계약 해지' 같은 다른 보고서는 일치하지 않아 자연히 빠진다.
    """

    return _PREFIX_TAG.sub("", str(report_nm or "")).strip() == TARGET_REPORT_NM


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """[start, end]를 달력월 구간으로 자른다(마지막 구간은 end로 클립).

    3개년 목록을 한 번에 받으면 페이지가 수백 장이라, 월 단위면 실패 시 되돌아갈
    범위도 작고 로그로 진행률도 보인다.
    """

    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        window_end = min(date(cursor.year, cursor.month, last_day), end)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def fetch_target_filings(client: DartClient, start: date, end: date) -> list[dict[str, Any]]:
    """[start, end] 접수분의 단일판매ㆍ공급계약체결(정정 포함) 목록 원본 행.

    Raises:
        QuotaExceeded: 일 호출 한도(020/021)에 걸린 경우.
    """

    rows: list[dict[str, Any]] = []
    page_no = 1
    while True:
        try:
            data = client.get_json(
                DISCLOSURE_LIST_PATH,
                {
                    "bgn_de": start.strftime("%Y%m%d"),
                    "end_de": end.strftime("%Y%m%d"),
                    "pblntf_ty": DISCLOSURE_KIND,
                    "page_no": str(page_no),
                    "page_count": str(LIST_PAGE_COUNT),
                },
            )
        except DartApiError as exc:
            if exc.status in QUOTA_STATUSES:
                raise QuotaExceeded(str(exc)) from exc
            raise
        batch = data.get("list") or []
        if not batch:
            break
        rows.extend(batch)
        if page_no >= int(data.get("total_page") or 0):
            break
        page_no += 1

    return [row for row in rows if is_target_report(row.get("report_nm"))]


def fetch_document_html(client: DartClient, rcept_no: str) -> str:
    """공시 원문 본문 문자열. 원문이 없으면(013/014) 빈 문자열.

    Raises:
        QuotaExceeded: 일 호출 한도(020/021)에 걸린 경우.
    """

    try:
        return client.get_document_text(rcept_no)
    except DartApiError as exc:
        if exc.status in QUOTA_STATUSES:
            raise QuotaExceeded(str(exc)) from exc
        if exc.status in NO_DATA_STATUSES:
            return ""
        raise
