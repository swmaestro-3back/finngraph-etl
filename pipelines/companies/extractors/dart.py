"""OpenDART 수집.

DART가 KIS로 메울 수 없는 두 구멍을 채운다.
- **비상장 법인**: KIS에는 아예 없다. DART에는 11만 4천 곳이 있다.
- **별도(OFS) 재무**: KIS는 연결만 준다. `fs_div`로 갈라 받는다.

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


def fetch_annual_report_receipts(
    corp_code: str,
    start: date,
    end: date,
    client: DartClient | None = None,
) -> list[dict[str, Any]]:
    """정기공시(pblntf_ty=A) 목록에서 사업보고서 접수 정보를 최신순으로 가져온다.

    분기·반기보고서도 같은 유형으로 오므로 보고서명으로 사업보고서만 거른다.
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

    return [
        row for row in (data.get("list") or []) if "사업보고서" in str(row.get("report_nm") or "")
    ]


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
