from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from pipelines.market_calendar.models import (
    CompanyRef,
    IpoFilingHead,
    KsdOfferingRef,
    LinkTarget,
    OfferingTerms,
    StoredFiling,
)
from pipelines.market_calendar.transformers.parsing import parse_date

GROUP_GENERAL = "일반사항"
GROUP_SECURITIES = "증권의종류"
GROUP_UNDERWRITERS = "인수인정보"
GROUP_FUND_USES = "자금의사용목적"
GROUP_SELLERS = "매출인에관한사항"
GROUP_PUTBACK = "일반청약자환매청구권"
GROUPS = (
    GROUP_GENERAL,
    GROUP_SECURITIES,
    GROUP_UNDERWRITERS,
    GROUP_FUND_USES,
    GROUP_SELLERS,
    GROUP_PUTBACK,
)

_KOREAN_DATE = re.compile(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일")
_TILDES = str.maketrans({"∼": "~", "～": "~", "〜": "~"})
_EMPTY = frozenset({"", "-"})


def clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return None if text in _EMPTY else text


def parse_korean_date(value: object) -> date | None:
    match = _KOREAN_DATE.search(str(value or ""))
    if match is None:
        return parse_date(value)
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def parse_korean_range(value: object) -> tuple[date | None, date | None]:
    parts = str(value or "").translate(_TILDES).split("~")
    if len(parts) == 2:
        return parse_korean_date(parts[0]), parse_korean_date(parts[1])
    single = parse_korean_date(value)
    return single, single


def parse_number(value: object) -> Decimal | None:
    text = str(value or "").strip().replace(",", "")
    if text in _EMPTY:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def parse_count(value: object) -> int | None:
    number = parse_number(value)
    if number is None or number != number.to_integral_value():
        return None
    return int(number)


def _latest_rows(groups: Mapping[str, list[dict[str, Any]]], title: str) -> list[dict[str, Any]]:
    rows = [row for row in groups.get(title) or [] if isinstance(row, dict)]
    if not rows:
        return []
    latest = max(str(row.get("rcept_no") or "") for row in rows)
    return [row for row in rows if str(row.get("rcept_no") or "") == latest]


def _security_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if "보통" in str(row.get("stksen") or ""):
            return row
    return rows[0] if rows else {}


def _underwriters(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        name = clean_text(row.get("actnmn"))
        if name is None:
            continue
        result.append(
            {
                "name": name,
                "role": clean_text(row.get("actsen")),
                "shares": parse_count(row.get("udtcnt")),
                "amount": parse_count(row.get("udtamt")),
                "method": clean_text(row.get("udtmth")),
            }
        )
    return result


def _fund_uses(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        purpose = clean_text(row.get("se"))
        amount = parse_count(row.get("amt"))
        if purpose is None or amount is None:
            continue
        result.append({"purpose": purpose, "amount": amount})
    return result


FUND_USE_SCALES = (1, 1_000, 1_000_000)
FUND_USE_RESCALE_RATIO = 500


def _scaled_fund_uses(
    uses: list[dict[str, Any]], offer_amount: Decimal | None
) -> list[dict[str, Any]]:
    total = sum(use["amount"] for use in uses)
    if total <= 0 or offer_amount is None or offer_amount <= 0:
        return uses
    if offer_amount / total < FUND_USE_RESCALE_RATIO:
        return uses
    scale = min(
        FUND_USE_SCALES, key=lambda unit: abs(math.log(float(offer_amount) / (total * unit)))
    )
    return [{**use, "amount": use["amount"] * scale} for use in uses]


def _sellers(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        holder = clean_text(row.get("hdr"))
        if holder is None:
            continue
        result.append(
            {
                "holder": holder,
                "relation": clean_text(row.get("rl_cmp")),
                "before": parse_count(row.get("bfsl_hdstk")),
                "sold": parse_count(row.get("slstk")),
                "after": parse_count(row.get("atsl_hdstk")),
            }
        )
    return result


def _putback(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        putback = {
            "reason": clean_text(row.get("grtrs")),
            "investors": clean_text(row.get("exavivr")),
            "shares": clean_text(row.get("grtcnt")),
            "period": clean_text(row.get("expd")),
            "price": clean_text(row.get("exprc")),
        }
        if any(value is not None for value in putback.values()):
            return putback
    return None


def to_offering_terms(groups: Mapping[str, list[dict[str, Any]]]) -> OfferingTerms | None:
    if not any(groups.get(title) for title in GROUPS):
        return None
    general = next(iter(_latest_rows(groups, GROUP_GENERAL)), {})
    security = _security_row(_latest_rows(groups, GROUP_SECURITIES))
    subscr_start, subscr_end = parse_korean_range(general.get("sbd"))
    offer_amount = parse_number(security.get("slta"))
    return OfferingTerms(
        subscr_start=subscr_start,
        subscr_end=subscr_end,
        pay_date=parse_korean_date(general.get("pymd")),
        offer_price=parse_number(security.get("slprc")),
        offer_shares=parse_count(security.get("stkcnt")),
        offer_amount=offer_amount,
        offer_method=clean_text(security.get("slmthn")),
        underwriters=_underwriters(_latest_rows(groups, GROUP_UNDERWRITERS)),
        fund_uses=_scaled_fund_uses(
            _fund_uses(_latest_rows(groups, GROUP_FUND_USES)), offer_amount
        ),
        sellers=_sellers(_latest_rows(groups, GROUP_SELLERS)),
        putback=_putback(_latest_rows(groups, GROUP_PUTBACK)),
    )


STATUS_FILED = "FILED"
STATUS_PRICED = "PRICED"
STATUS_WITHDRAWN = "WITHDRAWN"

FILING_CLASSES = frozenset({"E", "N"})
REGISTRATION_NAME = "증권신고서(지분증권)"
PROSPECTUS_NAME = "투자설명서"
WITHDRAWAL_NAME = "철회신고서"
PRICED_TAG = "[발행조건확정]"
DOCUMENTLESS_TAGS = ("[첨부정정]", "[첨부추가]", "[정정제출요구]")
SPAC_MARKERS = ("기업인수목적", "스팩")

_REPORT_TAGS = re.compile(r"^(?:\s*\[[^\]]*\])+")
_XML_STATUS = re.compile(r"<status>\s*(\d{3})\s*</status>")


def base_report_name(report_nm: object) -> str:
    return _REPORT_TAGS.sub("", str(report_nm or "")).strip()


def is_spac(name: object) -> bool:
    text = str(name or "")
    return any(marker in text for marker in SPAC_MARKERS)


def error_status(status: str, message: str) -> str:
    if status:
        return status
    match = _XML_STATUS.search(message or "")
    return match.group(1) if match else ""


def _text(row: Mapping[str, Any], key: str) -> str:
    return str(row.get(key) or "").strip()


def group_filing_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        rcept_no = _text(row, "rcept_no")
        if not rcept_no or not _text(row, "corp_code"):
            continue
        if _text(row, "corp_cls") not in FILING_CLASSES:
            continue
        unique[rcept_no] = dict(row)
    groups: dict[str, list[dict[str, Any]]] = {}
    for rcept_no in sorted(unique):
        row = unique[rcept_no]
        groups.setdefault(_text(row, "corp_code"), []).append(row)
    return groups


def _filed_on(row: Mapping[str, Any]) -> date | None:
    return parse_date(row.get("rcept_dt")) or parse_date(_text(row, "rcept_no")[:8])


def _is_document(row: Mapping[str, Any]) -> bool:
    name = _text(row, "report_nm")
    if any(tag in name for tag in DOCUMENTLESS_TAGS):
        return False
    return base_report_name(name) in (REGISTRATION_NAME, PROSPECTUS_NAME)


def _is_terms_source(row: Mapping[str, Any]) -> bool:
    name = _text(row, "report_nm")
    if PRICED_TAG in name or any(tag in name for tag in DOCUMENTLESS_TAGS):
        return False
    return base_report_name(name) == REGISTRATION_NAME


def derive_head(rows: Sequence[Mapping[str, Any]]) -> IpoFilingHead | None:
    ordered = sorted(
        (row for row in rows if _text(row, "rcept_no")), key=lambda row: row["rcept_no"]
    )
    registrations = [
        row for row in ordered if base_report_name(row.get("report_nm")) == REGISTRATION_NAME
    ]
    withdrawals = [row for row in ordered if WITHDRAWAL_NAME in _text(row, "report_nm")]
    if not registrations and not withdrawals:
        return None
    latest = ordered[-1]
    withdrawn = "철" in _text(latest, "rm") or (
        bool(withdrawals)
        and (
            not registrations
            or _text(withdrawals[-1], "rcept_no") > _text(registrations[-1], "rcept_no")
        )
    )
    if withdrawn:
        status = STATUS_WITHDRAWN
    elif any(PRICED_TAG in _text(row, "report_nm") for row in ordered):
        status = STATUS_PRICED
    else:
        status = STATUS_FILED
    first = next(
        (row for row in ordered if _text(row, "report_nm") == REGISTRATION_NAME), ordered[0]
    )
    first_filed_on = _filed_on(first)
    if first_filed_on is None:
        return None
    documents = [row for row in ordered if _is_document(row)]
    sources = [row for row in ordered if _is_terms_source(row)]
    corp_code = _text(latest, "corp_code")
    corp_name = _text(latest, "corp_name") or corp_code
    return IpoFilingHead(
        corp_code=corp_code,
        corp_name=corp_name,
        corp_cls=_text(latest, "corp_cls"),
        status=status,
        spac=is_spac(corp_name),
        first_rcept_no=_text(first, "rcept_no"),
        first_filed_on=first_filed_on,
        latest_rcept_no=_text(latest, "rcept_no"),
        latest_report_nm=_text(latest, "report_nm"),
        document_rcept_no=_text(documents[-1], "rcept_no") if documents else None,
        terms_rcept_no=_text(sources[-1], "rcept_no") if sources else None,
    )


def merge_stored(head: IpoFilingHead, stored: StoredFiling | None) -> IpoFilingHead:
    if stored is None or not stored.first_rcept_no or stored.first_rcept_no >= head.first_rcept_no:
        return head
    return replace(head, first_rcept_no=stored.first_rcept_no, first_filed_on=stored.first_filed_on)


def terms_current(head: IpoFilingHead, groups: Mapping[str, list[dict[str, Any]]]) -> bool:
    received = [
        str(row.get("rcept_no") or "")
        for title in GROUPS
        for row in groups.get(title) or []
        if isinstance(row, dict)
    ]
    if not any(received):
        return False
    return head.terms_rcept_no is None or max(received) >= head.terms_rcept_no


def needs_terms(head: IpoFilingHead, stored: StoredFiling | None) -> bool:
    if head.status == STATUS_WITHDRAWN:
        return False
    if stored is None:
        return True
    return stored.latest_rcept_no != head.latest_rcept_no or not stored.has_terms


def needs_profile(head: IpoFilingHead, company: CompanyRef | None) -> bool:
    return head.status != STATUS_WITHDRAWN and company is not None and not company.has_profile


def needs_description(
    head: IpoFilingHead, stored: StoredFiling | None, company: CompanyRef | None
) -> bool:
    if head.status == STATUS_WITHDRAWN or head.document_rcept_no is None or company is None:
        return False
    return stored is None or not stored.description_ready


MATCH_WINDOW_DAYS = 3

_NAME_NOISE = re.compile(r"주식회사|\(주\)|㈜|\(株\)|\s+")


def normalize_company_name(name: object) -> str:
    return _NAME_NOISE.sub("", str(name or "")).replace("기업인수목적", "스팩").lower()


def match_offering_ticker(
    name: str, subscr_start: date | None, offerings: Sequence[KsdOfferingRef]
) -> str | None:
    key = normalize_company_name(name)
    if subscr_start is None or not key:
        return None
    candidates = sorted(
        (abs((offering.subscr_start - subscr_start).days), offering.ticker)
        for offering in offerings
        if normalize_company_name(offering.name) == key
        and abs((offering.subscr_start - subscr_start).days) <= MATCH_WINDOW_DAYS
    )
    return candidates[0][1] if candidates else None


def link_tickers(
    targets: Sequence[LinkTarget],
    offerings: Sequence[KsdOfferingRef],
    listed: Mapping[str, str],
) -> dict[str, str]:
    changes: dict[str, str] = {}
    for target in targets:
        ticker = listed.get(target.corp_code) or match_offering_ticker(
            target.corp_name, target.subscr_start, offerings
        )
        if ticker and ticker != target.ticker:
            changes[target.corp_code] = ticker
    return changes
