"""공시 원문 HTML을 정규화된 레코드로 바꾼다.

단일판매ㆍ공급계약체결 서식은 시장(KOSPI/KOSDAQ)에 따라 다르고 정정 공시에는 정정신고
블록이 앞에 붙는다. 그래서 표를 먼저 서식 무관하게 격자로 펼친 뒤에야 정규화 스키마
(fields)로 매핑한다. 펼친 격자는 메모리에만 있다 — 레코드에는 fields 만 남는다.

순수 함수만 있다. DB·HTTP 는 extractors/loaders 의 몫이다.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from bs4 import BeautifulSoup

from pipelines.common.utils.time import now_kst
from pipelines.disclosures.models import DisclosureRecord
from pipelines.disclosures.transformers.counterparty import CorpMaster

VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

# fields 스키마의 버전. 매핑 규칙이 바뀌면 올린다. 프로토타입의 버전과 무관하게 1부터다.
# v2: 계약 구분·계약명·계약상대(역매칭 포함)·계약 기간을 일반 컬럼으로 승격, fields 에서 제거.
PARSER_VERSION = 2

_NUMBERED = re.compile(r"^\s*\d+\s*\.\s*")
_LEADING_DASH = re.compile(r"^\s*[-ㆍ·]\s*")
_DATE = re.compile(r"^(\d{4})[-./](\d{1,2})[-./](\d{1,2})$")
_AMOUNT = re.compile(r"^-?[\d,]+$")
_EMPTY = {"", "-", "–"}

CORRECTION_HEADER = ("정정항목", "정정전", "정정후")

# DART corp_cls 코드 -> 시장 이름
MARKETS = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "UNLISTED"}


def market(corp_cls: str | None) -> str | None:
    return MARKETS.get(str(corp_cls).strip().upper()) if corp_cls else None


# ------------------------------------------------------------------ utilities
def canon(label: str) -> str:
    """라벨 비교 키: 번호·선행 대시·공백·ㆍ 구분자를 걷어낸다."""

    text = _LEADING_DASH.sub("", _NUMBERED.sub("", str(label)))
    return re.sub(r"\s+", "", text).replace("ㆍ", "").replace("·", "")


def clean(text: str) -> str:
    return " ".join(str(text).split())


def is_blank(value: str | None) -> bool:
    return value is None or clean(value) in _EMPTY


def to_number(value: str) -> int | None:
    text = clean(value).replace(" ", "")
    if not _AMOUNT.match(text):
        return None
    return int(text.replace(",", ""))


def to_float(value: str) -> float | None:
    text = clean(value).replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def to_date(value: str) -> str | None:
    m = _DATE.match(clean(value))
    if not m:
        return None
    year, month, day = (int(g) for g in m.groups())
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


# ------------------------------------------------------------------- html/grid
def table_grid(table) -> list[list[str]]:
    """rowspan/colspan 셀을 복제해 표를 직사각형 격자로 펼친다."""

    rows: list[list[str]] = []
    spill: dict[tuple[int, int], str] = {}
    for r, tr in enumerate(table.find_all("tr")):
        row: list[str] = []
        col = 0
        while (r, col) in spill:
            row.append(spill.pop((r, col)))
            col += 1
        for cell in tr.find_all(["td", "th"], recursive=False):
            text = clean(cell.get_text(" ", strip=True))
            colspan = int(cell.get("colspan", 1) or 1)
            rowspan = int(cell.get("rowspan", 1) or 1)
            for i in range(colspan):
                for j in range(rowspan):
                    if j == 0:
                        row.append(text)
                    else:
                        spill[(r + j, col + i)] = text
            col += colspan
            while (r, col) in spill:
                row.append(spill.pop((r, col)))
                col += 1
        rows.append(row)
    return rows


def _row_to_entry(row: list[str], section: str) -> tuple[str, str, str] | None:
    """격자 한 행을 (section, label, value)로. 머리글·본문 전용 행은 None."""

    if len(row) == 2:
        return section, row[0], row[1]
    if len(row) < 3:
        return None
    first, second, last = row[0], row[1], row[-1]
    if first == second == last:
        return None  # 전체 폭 행: 섹션 머리글 또는 자유 본문
    if second == last:  # 값이 오른쪽 여러 칸에 걸침
        return section, first, last
    if first == second:  # 라벨이 왼쪽 여러 칸에 걸침
        return section, first, last
    return first, second, last


def extract_rows(tables) -> list[dict[str, Any]]:
    """표들을 순서 있는 {section, label, value} 항목으로 펼친다."""

    entries: list[dict[str, Any]] = []
    section = ""
    text_target: dict[str, Any] | None = None

    for table in tables:
        grid = table_grid(table)
        idx = 0
        while idx < len(grid):
            row = grid[idx]
            idx += 1
            if not row or all(is_blank(cell) for cell in row):
                continue

            # 정정사항 소표: 머리글 행 다음에 정정전/정정후 행이 이어진다.
            if len(row) >= 3 and tuple(clean(c) for c in row[:3]) == CORRECTION_HEADER:
                while idx < len(grid) and len(grid[idx]) >= 3:
                    item = grid[idx]
                    idx += 1
                    if all(is_blank(c) for c in item):
                        continue
                    entries.append(
                        {
                            "section": "정정사항",
                            "label": clean(item[0]),
                            "value": clean(item[-1]),
                            "before": clean(item[1]),
                            "after": clean(item[-1]),
                        }
                    )
                text_target = None
                continue

            first = clean(row[0])
            if len(row) >= 3 and first == clean(row[1]) == clean(row[-1]):
                # 전체 폭 행: 섹션 머리글이거나, 그 뒤로 이어지는 본문이다.
                if _NUMBERED.match(first) or first.startswith("※"):
                    section = first
                    text_target = {"section": section, "label": first, "value": ""}
                    entries.append(text_target)
                elif text_target is not None:
                    joined = f"{text_target['value']} {first}".strip()
                    text_target["value"] = joined
                continue

            entry = _row_to_entry(row, section)
            if entry is None:
                continue
            entry_section, label, value = (clean(x) for x in entry)
            if _NUMBERED.match(label) or label.startswith("※"):
                section = label
            elif _NUMBERED.match(entry_section):
                section = entry_section
            entries.append({"section": section, "label": label, "value": value})
            text_target = None
    return entries


def split_scopes(soup) -> tuple[list, list]:
    """표들을 (정정신고 블록, 본문)으로 가른다.

    정정 공시는 정정신고 블록이 앞에 붙는다. 본문은 "1. 판매ㆍ공급계약 ..." 행을 담은
    첫 표부터다.
    """

    tables = soup.find_all("table")
    for i, table in enumerate(tables):
        text = canon(table.get_text(" ", strip=True))
        if text.startswith("1.판매공급계약") or canon(text).startswith("판매공급계약"):
            return tables[:i], tables[i:]
    return [], tables


# ---------------------------------------------------------------- field mapping
_COUNTERPARTY_SECTIONS = ("계약상대", "계약상대방")

_SIMPLE_FIELDS = {
    "판매공급계약구분": "contract_type",
    "판매공급계약내용": "contract_type",
    "체결계약명": "contract_name",
    "계약금액(원)": "contract_amount",
    "계약금액총액(원)": "contract_amount",
    "확정계약금액": "fixed_amount",
    "조건부계약금액": "conditional_amount",
    "조건부계약여부": "is_conditional",
    "최근매출액(원)": "recent_sales",
    "매출액대비(%)": "sales_ratio",
    "대규모법인여부": "is_large_corp",
    "계약상대": "counterparty",
    "계약상대방": "counterparty",
    "회사와의관계": "counterparty_relation",
    "주요사업": "counterparty_business",
    "회사와최근3년간동종계약이행여부": "counterparty_prior_contract",
    "판매공급지역": "region",
    "시작일": "start_date",
    "종료일": "end_date",
    "계약(수주)일자": "order_date",
    "수주일자": "order_date",
}

_DICT_SECTIONS = {
    "주요계약조건": "conditions",
    "판매공급방식": "supply_method",
    "공시유보관련내용": "reservation",
}

_NUMERIC_FIELDS = {
    "contract_amount",
    "fixed_amount",
    "conditional_amount",
    "recent_sales",
    "counterparty_recent_sales",
}
_DATE_FIELDS = {"start_date", "end_date", "order_date"}

EMPTY_FIELDS: dict[str, Any] = {
    "contract_type": None,
    "contract_name": None,
    "contract_amount": None,
    "fixed_amount": None,
    "conditional_amount": None,
    "is_conditional": None,
    "recent_sales": None,
    "sales_ratio": None,
    "is_large_corp": None,
    "counterparty": None,
    "counterparty_corp_name": None,
    "counterparty_corp_code": None,
    "counterparty_ticker": None,
    "counterparty_company_id": None,
    "counterparty_match_rule": None,
    "counterparty_relation": None,
    "counterparty_business": None,
    "counterparty_recent_sales": None,
    "counterparty_prior_contract": None,
    "region": None,
    "start_date": None,
    "end_date": None,
    "order_date": None,
    "conditions": {},
    "supply_method": {},
    "reservation": {},
    "note": None,
    "related_disclosures": None,
    "correction": None,
}


def _note_section(key: str) -> bool:
    return key.startswith("기타투자판단")


def build_fields(entries: list[dict[str, Any]]) -> dict[str, Any]:
    fields: dict[str, Any] = {
        k: (dict(v) if isinstance(v, dict) else v) for k, v in EMPTY_FIELDS.items()
    }
    notes: list[str] = []

    for entry in entries:
        label_key = canon(entry["label"])
        section_key = canon(entry["section"])
        value = entry["value"]

        if section_key in _DICT_SECTIONS and label_key != section_key:
            fields[_DICT_SECTIONS[section_key]][clean(entry["label"])] = value
            continue
        if _note_section(section_key):
            # 본문이 여러 행에 걸쳐 나뉠 수 있다. 순서대로 전부 모은다.
            if not is_blank(value) and value not in notes:
                notes.append(value)
            continue
        if label_key.startswith("※관련공시"):
            fields["related_disclosures"] = None if is_blank(value) else value
            continue

        field = _SIMPLE_FIELDS.get(label_key)
        if field is None:
            continue
        # 계약상대(방) 섹션 아래의 매출·관계 행은 계약상대에 대한 설명이다.
        if any(section_key.startswith(s) for s in _COUNTERPARTY_SECTIONS):
            if field == "recent_sales":
                field = "counterparty_recent_sales"
        if is_blank(value):
            continue
        if field in _NUMERIC_FIELDS:
            fields[field] = to_number(value)
        elif field == "sales_ratio":
            fields[field] = to_float(value)
        elif field in _DATE_FIELDS:
            fields[field] = to_date(value) or clean(value)
        else:
            fields[field] = clean(value)

    if notes:
        fields["note"] = " ".join(notes)
    return fields


def build_correction(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not entries:
        return None
    correction: dict[str, Any] = {"items": []}
    for entry in entries:
        if entry.get("section") == "정정사항":
            correction["items"].append(
                {
                    "정정항목": entry["label"],
                    "정정전": entry.get("before", ""),
                    "정정후": entry.get("after", ""),
                }
            )
        else:
            correction[clean(entry["label"])] = entry["value"]
    return correction


# ------------------------------------------------------------------ entrypoint
def parse_contract_fields(html: str) -> tuple[dict[str, Any], bool]:
    """본문 HTML -> (fields, is_correction). 계약상대 역매칭 전 상태다."""

    soup = BeautifulSoup(html, "lxml")
    correction_tables, body_tables = split_scopes(soup)

    body_entries = extract_rows(body_tables)
    correction_entries = extract_rows(correction_tables)

    fields = build_fields(body_entries)
    fields["correction"] = build_correction(correction_entries)
    return fields, bool(correction_entries)


# 일반 컬럼으로 승격된 항목 — fields 에서 꺼내 레코드 속성으로 옮긴다.
_PROMOTED_TEXT_FIELDS = (
    "contract_type",
    "contract_name",
    "counterparty",
    "counterparty_corp_name",
    "counterparty_corp_code",
    "counterparty_ticker",
)
_PROMOTED_DATE_FIELDS = ("start_date", "end_date", "order_date")

# 정정신고 블록에서 컬럼으로 승격하는 항목. 라벨은 canon 키로 비교한다("2. 정정관련
# 공시서류제출일" 류 번호·공백 무시). 블록 원문(fields["correction"])은 그대로 남긴다 —
# 정정사항(정정전/정정후) 목록은 구조가 가변적이라 승격하지 않는다.
_CORRECTION_PROMOTED = {
    "정정관련공시서류": "correction_target_report",
    "정정관련공시서류제출일": "correction_target_date",
    "정정사유": "correction_reason",
}


def _promote_correction(correction: dict[str, Any] | None) -> dict[str, Any]:
    promoted: dict[str, Any] = {field: None for field in _CORRECTION_PROMOTED.values()}
    if not correction:
        return promoted
    for label, value in correction.items():
        if not isinstance(value, str):
            continue  # items(정정사항 목록)는 승격 대상이 아니다
        field = _CORRECTION_PROMOTED.get(canon(label))
        if field is None or is_blank(value):
            continue
        if field == "correction_target_date":
            iso = to_date(value)
            promoted[field] = date.fromisoformat(iso) if iso else None
        else:
            promoted[field] = clean(value)
    return promoted


def _pop_promoted_date(fields: dict[str, Any], key: str) -> date | None:
    """승격 날짜 항목을 fields 에서 꺼낸다.

    파서는 "계약 종료시" 같은 비정형 기재도 텍스트로 통과시키므로, ISO 형식만 DATE 컬럼으로
    승격하고 비정형 텍스트는 fields 에 남긴다 — 타입과 원문 보존을 맞바꾸지 않는다.
    """

    value = fields.get(key)
    if value is None:
        fields.pop(key, None)
        return None
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError:
        return None  # 비정형 기재 — 컬럼은 NULL, 원문은 fields 에 남긴다
    fields.pop(key, None)
    return parsed


def build_disclosure(
    filing: dict[str, Any],
    html: str,
    corp_master: CorpMaster,
) -> DisclosureRecord:
    """목록 API 행 + 원문 HTML + 법인 마스터 -> 적재 단위 레코드.

    제출사의 company_id/ticker 는 corp_code 로, 계약상대는 이름 역매칭으로 채운다.
    승격 항목은 fields 에서 빠져 일반 컬럼으로 들어간다.
    """

    fields, is_correction = parse_contract_fields(html)
    fields.update(corp_master.resolve_counterparty(fields["counterparty"]))

    promoted = {key: fields.pop(key, None) for key in _PROMOTED_TEXT_FIELDS}
    promoted_dates = {key: _pop_promoted_date(fields, key) for key in _PROMOTED_DATE_FIELDS}
    correction = _promote_correction(fields.get("correction"))

    rcept_no = str(filing["rcept_no"])
    corp_code = str(filing.get("corp_code") or "")
    filer = corp_master.resolve_filer(corp_code)

    return DisclosureRecord(
        rcept_no=rcept_no,
        corp_code=corp_code,
        company_id=filer.company_id if filer else None,
        ticker=filer.ticker if filer else None,
        corp_cls=market(filing.get("corp_cls")),
        report_nm=clean(filing.get("report_nm") or "") or None,
        is_correction=is_correction,
        correction_target_report=correction["correction_target_report"],
        correction_target_date=correction["correction_target_date"],
        correction_reason=correction["correction_reason"],
        # 정정공시의 체인 루트는 원문만으로 알 수 없다(부모의 제출일만 있고 접수번호가
        # 없다) — link job 의 체인 해소가 채운다.
        original_rcept_no=None if is_correction else rcept_no,
        rcept_dt=datetime.strptime(str(filing["rcept_dt"]), "%Y%m%d").date(),
        flr_nm=filing.get("flr_nm"),
        link=VIEWER_URL.format(rcept_no=rcept_no),
        contract_type=promoted["contract_type"],
        contract_name=promoted["contract_name"],
        counterparty=promoted["counterparty"],
        counterparty_corp_name=promoted["counterparty_corp_name"],
        counterparty_corp_code=promoted["counterparty_corp_code"],
        counterparty_ticker=promoted["counterparty_ticker"],
        start_date=promoted_dates["start_date"],
        end_date=promoted_dates["end_date"],
        order_date=promoted_dates["order_date"],
        fields=fields,
        meta={
            "fetched_at": now_kst().isoformat(timespec="seconds"),
            "parser_version": PARSER_VERSION,
        },
    )
