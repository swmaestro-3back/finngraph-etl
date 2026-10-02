from __future__ import annotations

from datetime import date
from typing import Any

from pipelines.common.clients.dart import (
    NO_DATA_STATUSES,
    QUOTA_STATUSES,
    DartApiError,
    DartClient,
    QuotaExceeded,
)
from pipelines.companies.extractors.dart import fetch_company_profile
from pipelines.companies.models import CompanyProfile
from pipelines.market_calendar.transformers.offerings import error_status

LIST_PATH = "list.json"
OFFERING_PATH = "estkRs.json"
IPO_FILING_KIND = "C001"
LIST_PAGE_COUNT = 100


def _quota(exc: DartApiError) -> bool:
    return error_status(exc.status, exc.message) in QUOTA_STATUSES


def fetch_ipo_filing_rows(client: DartClient, start: date, end: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_no = 1
    while True:
        try:
            data = client.get_json(
                LIST_PATH,
                {
                    "bgn_de": start.strftime("%Y%m%d"),
                    "end_de": end.strftime("%Y%m%d"),
                    "pblntf_detail_ty": IPO_FILING_KIND,
                    "page_no": str(page_no),
                    "page_count": str(LIST_PAGE_COUNT),
                },
            )
        except DartApiError as exc:
            if _quota(exc):
                raise QuotaExceeded(str(exc)) from exc
            raise
        batch = data.get("list") or []
        if not batch:
            break
        rows.extend(batch)
        if page_no >= int(data.get("total_page") or 0):
            break
        page_no += 1
    return rows


def fetch_offering_groups(
    client: DartClient, corp_code: str, start: date, end: date
) -> dict[str, list[dict[str, Any]]]:
    try:
        data = client.get_json(
            OFFERING_PATH,
            {
                "corp_code": corp_code,
                "bgn_de": start.strftime("%Y%m%d"),
                "end_de": end.strftime("%Y%m%d"),
            },
        )
    except DartApiError as exc:
        if _quota(exc):
            raise QuotaExceeded(str(exc)) from exc
        raise
    groups: dict[str, list[dict[str, Any]]] = {}
    for group in data.get("group") or []:
        title = str(group.get("title") or "").strip()
        if title:
            groups[title] = list(group.get("list") or [])
    return groups


def fetch_profile(client: DartClient, corp_code: str) -> CompanyProfile | None:
    try:
        return fetch_company_profile(corp_code, client=client)
    except DartApiError as exc:
        if _quota(exc):
            raise QuotaExceeded(str(exc)) from exc
        raise


def fetch_document(client: DartClient, rcept_no: str) -> str:
    try:
        return client.get_document_text(rcept_no)
    except DartApiError as exc:
        status = error_status(exc.status, exc.message)
        if status in QUOTA_STATUSES:
            raise QuotaExceeded(str(exc)) from exc
        if status in NO_DATA_STATUSES:
            return ""
        raise
