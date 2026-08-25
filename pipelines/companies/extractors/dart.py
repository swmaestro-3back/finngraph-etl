"""OpenDART 수집.

DART가 KIS로 메울 수 없는 두 구멍을 채운다.
- 비상장 법인: KIS에는 아예 없다. DART에는 11만 4천 곳이 있다.
- 별도(OFS) 재무: KIS는 연결만 준다. `fs_div`로 갈라 받는다.

corpCode.xml은 30MB 남짓한 단일 XML이라 통째로 파싱하면 메모리를 크게 먹는다.
iterparse로 한 항목씩 읽고 즉시 버린다.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from xml.etree import ElementTree

from pipelines.common.dart import DartClient, get_dart_client
from pipelines.companies.models import CompanyProfile, DartCorp

CORP_CODE_PATH = "corpCode.xml"
COMPANY_PATH = "company.json"
FINANCIAL_PATH = "fnlttSinglAcntAll.json"
DISCLOSURE_LIST_PATH = "list.json"

# 보고서 코드. 11011=사업보고서(연간), 11012=반기, 11013=1분기, 11014=3분기.
REPORT_CODE_ANNUAL = "11011"

# 재무제표 구분. CFS=연결, OFS=별도.
FS_DIV_CONSOLIDATED = "CFS"
FS_DIV_SEPARATE = "OFS"


def fetch_corp_codes(client: DartClient | None = None) -> list[DartCorp]:
    """corpCode.xml 전량을 파싱해 반환한다.

    Returns:
        list[DartCorp]: 전체 법인. 2026-08 기준 약 11만 8천 건.
    """

    client = client or get_dart_client()
    members = client.get_zip_members(CORP_CODE_PATH)

    xml_members = [content for name, content in members.items() if name.lower().endswith(".xml")]
    if len(xml_members) != 1:
        raise ValueError(f"corpCode zip에서 XML 하나를 기대했으나 {list(members)}가 왔다")

    return parse_corp_codes(xml_members[0])


def parse_corp_codes(content: bytes) -> list[DartCorp]:
    """corpCode.xml 본문을 DartCorp 목록으로 만든다."""

    corps: list[DartCorp] = []
    root = ElementTree.fromstring(content)
    for element in root.iter("list"):
        corp_code = _text(element, "corp_code")
        name = _text(element, "corp_name")
        if not corp_code or not name:
            continue

        corps.append(
            DartCorp(
                corp_code=corp_code,
                name=name,
                name_eng=_text(element, "corp_eng_name"),
                # 원본은 상장 이력이 없으면 공백 한 칸을 넣는다. strip 후 빈 값은 None이다.
                stock_code=_text(element, "stock_code"),
                modify_date=_parse_date(_text(element, "modify_date")),
            )
        )
        element.clear()

    return corps


def fetch_company_profile(
    corp_code: str, client: DartClient | None = None
) -> CompanyProfile | None:
    """기업개황. 없으면 None."""

    client = client or get_dart_client()
    data = client.get_json(COMPANY_PATH, {"corp_code": corp_code})
    if not data:
        return None

    return CompanyProfile(
        corp_code=corp_code,
        name=str(data.get("corp_name") or "").strip(),
        name_eng=_clean(data.get("corp_name_eng")),
        ceo_name=_clean(data.get("ceo_nm")),
        industry_code=_clean(data.get("induty_code")),
        established_on=_parse_date(_clean(data.get("est_dt"))),
        fiscal_month=_clean(data.get("acc_mt")),
        homepage=_clean(data.get("hm_url")),
        address=_clean(data.get("adres")),
    )


def fetch_financial_statements(
    corp_code: str,
    business_year: str,
    fs_div: str,
    report_code: str = REPORT_CODE_ANNUAL,
    client: DartClient | None = None,
) -> list[dict[str, Any]]:
    """단일회사 전체 재무제표 원본 행.

    Args:
        corp_code (str): DART 고유번호.
        business_year (str): 'YYYY' 사업연도.
        fs_div (str): 'CFS'(연결) 또는 'OFS'(별도).
        report_code (str): 보고서 코드. 기본은 사업보고서(연간).
        client (DartClient | None): 재사용할 클라이언트.

    Returns:
        list[dict]: 계정 행. 데이터가 없으면 빈 리스트.
    """

    client = client or get_dart_client()
    data = client.get_json(
        FINANCIAL_PATH,
        {
            "corp_code": corp_code,
            "bsns_year": business_year,
            "reprt_code": report_code,
            "fs_div": fs_div,
        },
    )
    return list(data.get("list") or [])


# 정기공시 중 사업 설명이 실리는 보고서. 셋 다 「II. 사업의 내용 → 1. 사업의 개요」를
# 같은 서식으로 담는다. 신규 상장사는 첫 사업보고서 전이라 반기·분기만 있고, 사업보고서가
# 있어도 반기·분기가 더 최신이라 셋을 함께 본다.
PERIODIC_REPORT_NAMES = ("사업보고서", "반기보고서", "분기보고서")

# 첨부파일만 교체한 공시는 본문이 없거나 감사보고서가 온다. 나머지 접두어는 거르지
# 않는다 — [기재정정]은 정기공시의 9.4%라 잘못 거르면 손실이 크다.
SKIP_REPORT_PREFIXES = ("[첨부정정]",)


def fetch_periodic_report_receipts(
    corp_code: str,
    start: date,
    end: date,
    client: DartClient | None = None,
) -> list[dict[str, Any]]:
    """정기공시(pblntf_ty=A) 접수 정보를 최신순으로 가져온다.

    사업·반기·분기보고서를 모두 담는다. 종류로 우선순위를 두지 않고 접수일 순서를 쓴다 —
    회사가 무엇을 하는지는 가장 최근 보고서가 가장 정확하다.

    본문이 없는 [첨부정정]은 여기서 뺀다. 남겨 두면 목록 맨 앞을 차지해 조회가 한 번
    헛돈다.

    Args:
        corp_code (str): DART 법인 코드.
        start (date): 조회 시작일.
        end (date): 조회 종료일.
        client (DartClient | None): 재사용할 클라이언트.

    Returns:
        list[dict[str, Any]]: 접수 정보. list.json이 최신순으로 주므로 순서를 유지한다.
    """

    client = client or get_dart_client()
    data = client.get_json(
        DISCLOSURE_LIST_PATH,
        {
            "corp_code": corp_code,
            "bgn_de": start.strftime("%Y%m%d"),
            "end_de": end.strftime("%Y%m%d"),
            "pblntf_ty": "A",
            "page_count": "50",
        },
    )

    receipts = []
    for row in data.get("list") or []:
        name = str(row.get("report_nm") or "")
        if name.startswith(SKIP_REPORT_PREFIXES):
            continue
        if any(kind in name for kind in PERIODIC_REPORT_NAMES):
            receipts.append(row)
    return receipts


def fetch_document_text(rcept_no: str, client: DartClient | None = None) -> str:
    """공시 원본 본문 XML을 문자열로 돌려준다.

    본문 선별·디코딩 로직은 DartClient.get_document_text에 있다.
    """

    client = client or get_dart_client()
    return client.get_document_text(rcept_no)


def _text(element: ElementTree.Element, tag: str) -> str | None:
    child = element.find(tag)
    if child is None:
        return None
    return _clean(child.text)


def _clean(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_date(value: str | None) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None
