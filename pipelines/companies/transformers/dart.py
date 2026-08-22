"""DART 응답 정규화.

두 가지를 한다.

1. **법인명 정규화.** `(주)비바리퍼블리카`와 `비바리퍼블리카`가 같은 법인임을 알아보게 만든다.
   부분 문자열 매칭은 쓰면 안 된다 — `컬리`가 `컬리넌종합건설`에, `당근`이 `당근페이`에 걸린다.
   정규화한 뒤 **정확히** 일치하는 것만 같은 법인으로 본다.

2. **XBRL 계정 매핑.** account_id로 잡고, 없으면 account_nm으로 폴백한다. 삼성전자 기준
   account_id 미사용 행이 3% 수준이라 폴백 없이는 그만큼 조용히 빈다.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from pipelines.common.logging import get_logger
from pipelines.companies.models import CompanyFinancial

logger = get_logger(__name__)

# 법인 형태 표기. 이름 앞뒤 어디에나 붙는다.
_LEGAL_FORMS = (
    "주식회사",
    "유한회사",
    "유한책임회사",
    "합자회사",
    "합명회사",
    "재단법인",
    "사단법인",
    "의료법인",
    "학교법인",
)

_BRACKET_PATTERN = re.compile(r"[(（\[][^)）\]]*[)）\]]")
_NON_WORD_PATTERN = re.compile(r"[\s\-·.,'\"&]+")

# XBRL 표준 계정. 3-13 스파이크에서 7개 전부 실측 확인됐다.
ACCOUNT_ID_MAP = {
    "ifrs-full_Revenue": "revenue",
    "dart_OperatingIncomeLoss": "operating_income",
    "ifrs-full_OperatingIncomeLoss": "operating_income",
    "ifrs-full_ProfitLoss": "net_income",
    "ifrs-full_Assets": "total_assets",
    "ifrs-full_Liabilities": "total_liabilities",
    "ifrs-full_Equity": "total_equity",
}

# account_id가 비어 오는 행(약 3%)을 위한 계정명 폴백.
ACCOUNT_NAME_MAP = {
    "매출액": "revenue",
    "수익(매출액)": "revenue",
    "영업수익": "revenue",
    "영업이익": "operating_income",
    "영업이익(손실)": "operating_income",
    "당기순이익": "net_income",
    "당기순이익(손실)": "net_income",
    "자산총계": "total_assets",
    "부채총계": "total_liabilities",
    "자본총계": "total_equity",
}

# 원화로 보고한 재무만 받는다.
#
# 응답의 currency는 보고 통화이고, 금액은 그 통화 단위 그대로 온다. 국내 상장 외국기업이
# USD로 보고하는 경우가 있는데(글로벌에스엠 900070), 환산 없이 넣으면 환율 배수만큼 어긋난다
# — 실측으로 매출 90,947,338 USD가 원화 0.9억으로 적재됐고 실제는 1,293억이었다.
#
# 지금은 버리고 로그로만 남긴다. 값이 없는 것보다 틀린 값이 화면에 나가는 쪽이 나쁘고,
# 같은 법인의 KIS 재무는 원화로 오므로 실질 손실이 없다. 제대로 환산하려면 자산은 기말환율,
# 손익은 평균환율을 따로 써야 해서 환율 원천과 통화 컬럼이 함께 필요하다.
REPORTING_CURRENCY = "KRW"

# 계정별로 값을 읽어도 되는 재무제표(sj_div)와 그 우선순위.
#
# **한 응답에 다섯 개 표가 함께 온다** — BS(재무상태표) · IS(손익계산서) ·
# CIS(포괄손익계산서) · CF(현금흐름표) · SCE(자본변동표). 같은 account_id가 여러 표에
# 나오는데 뜻이 다르다. 경방 2025년 응답에서 ifrs-full_Equity가 8번 나오는데, BS의
# 8,118억이 자본총계이고 SCE의 137억·-209억은 자본의 **증감 내역** 한 줄씩이다.
# 표를 안 가리면 배당으로 줄어든 금액을 자본총계로 넣게 된다.
#
# 손익 항목에 CIS를 폴백으로 두는 이유는 IS 없이 포괄손익계산서만 제출하는 기업이 있어서다.
# 앞에 적힌 표를 우선한다.
FIELD_STATEMENTS = {
    "revenue": ("IS", "CIS"),
    "operating_income": ("IS", "CIS"),
    "net_income": ("IS", "CIS"),
    "total_assets": ("BS",),
    "total_liabilities": ("BS",),
    "total_equity": ("BS",),
}


def normalize_corp_name(name: str) -> str:
    """법인명을 매칭용 키로 정규화한다.

    괄호 표기·법인 형태·공백·구두점을 지운다. `(주)`와 `㈜`, `주식회사`가 모두 사라지고
    `SK 하이닉스`와 `SK하이닉스`가 같은 키가 된다.

    Args:
        name (str): 원본 법인명.

    Returns:
        str: 정규화 키. 소문자로 접는다.
    """

    text = _BRACKET_PATTERN.sub("", name or "")
    text = text.replace("㈜", "").replace("㈐", "")
    for form in _LEGAL_FORMS:
        text = text.replace(form, "")
    text = _NON_WORD_PATTERN.sub("", text)
    return text.strip().lower()


def build_dart_financials(
    company_id: int,
    rows: list[dict[str, Any]],
    fs_div: str,
    fiscal_month: str | None = None,
) -> list[CompanyFinancial]:
    """단일회사 전체 재무제표 응답을 적재 단위로 접는다.

    한 응답에 계정이 수백 줄 들어 있고 우리가 쓰는 것은 6개다. 접수번호(rcept_no)가
    같은 행끼리 하나로 접는다.

    Args:
        company_id (int): 대상 법인 id.
        rows (list[dict]): fnlttSinglAcntAll 응답의 list.
        fs_div (str): 'CFS'(연결) 또는 'OFS'(별도). **응답에 이 필드가 없으므로**
            요청할 때 쓴 값을 그대로 넘겨야 한다. 응답에서 읽으려 하면 항상 비어서
            연결·별도가 같은 키로 접히고 나중 것이 앞선 것을 덮어쓴다.
        fiscal_month (str | None): 결산월 'MM'. 없으면 12월로 본다.

    Returns:
        list[CompanyFinancial]: 접수번호별 1건씩.
    """

    normalized_fs_div = str(fs_div or "").strip().upper() or "CFS"

    # (rcept_no, business_year) → {field: (표 우선순위, 금액)}
    grouped: dict[tuple[str, str], dict[str, tuple[int, int]]] = {}

    skipped_currencies: set[str] = set()

    for row in rows:
        currency = str(row.get("currency") or REPORTING_CURRENCY).strip().upper()
        if currency != REPORTING_CURRENCY:
            skipped_currencies.add(currency)
            continue

        field = _resolve_field(row)
        if field is None:
            continue

        rank = _statement_rank(field, row.get("sj_div"))
        if rank is None:
            continue

        amount = _parse_amount(row.get("thstrm_amount"))
        if amount is None:
            continue

        business_year = str(row.get("bsns_year") or "").strip()
        if not business_year:
            continue

        rcept_no = str(row.get("rcept_no") or "").strip()
        bucket = grouped.setdefault((rcept_no, business_year), {})
        # 우선순위가 높은(숫자가 작은) 표의 값을 남긴다. 같은 표에서 여러 번 나오면
        # 먼저 온 것을 쓴다 — 응답 순서에 기대지 않으려고 표를 먼저 가린다.
        current = bucket.get(field)
        if current is None or rank < current[0]:
            bucket[field] = (rank, amount)

    if skipped_currencies:
        logger.warning(
            "원화가 아닌 보고 통화라 건너뜀: company_id=%s fs_div=%s currency=%s",
            company_id,
            normalized_fs_div,
            ",".join(sorted(skipped_currencies)),
        )

    financials: list[CompanyFinancial] = []
    for (rcept_no, business_year), values in grouped.items():
        amounts = {field: amount for field, (_, amount) in values.items()}
        financials.append(
            CompanyFinancial(
                company_id=company_id,
                source="DART",
                fs_div=normalized_fs_div,
                fiscal_yymm=f"{business_year}{_fiscal_month(fiscal_month)}",
                period_type="A",
                disclosed_at=_disclosed_at(rcept_no),
                rcept_no=rcept_no or None,
                revenue=amounts.get("revenue"),
                operating_income=amounts.get("operating_income"),
                net_income=amounts.get("net_income"),
                total_assets=amounts.get("total_assets"),
                total_liabilities=amounts.get("total_liabilities"),
                total_equity=amounts.get("total_equity"),
            )
        )

    return sorted(financials, key=lambda financial: (financial.fiscal_yymm, financial.fs_div))


def _resolve_field(row: dict[str, Any]) -> str | None:
    account_id = str(row.get("account_id") or "").strip()
    if account_id in ACCOUNT_ID_MAP:
        return ACCOUNT_ID_MAP[account_id]

    account_nm = str(row.get("account_nm") or "").strip()
    return ACCOUNT_NAME_MAP.get(account_nm)


def _statement_rank(field: str, sj_div: object) -> int | None:
    """그 계정을 이 표에서 읽어도 되는지, 읽는다면 우선순위는 몇 번째인지.

    Returns:
        int | None: 우선순위(작을수록 우선). 읽으면 안 되는 표면 None.
    """

    allowed = FIELD_STATEMENTS.get(field)
    if not allowed:
        return None

    code = str(sj_div or "").strip().upper()
    return allowed.index(code) if code in allowed else None


def _parse_amount(value: object) -> int | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _fiscal_month(fiscal_month: str | None) -> str:
    text = str(fiscal_month or "").strip()
    if len(text) == 1:
        text = f"0{text}"
    return text if len(text) == 2 and text.isdigit() else "12"


def _disclosed_at(rcept_no: str) -> date | None:
    """접수번호 앞 8자리가 접수일이다. point-in-time의 근거가 된다."""

    if len(rcept_no) < 8 or not rcept_no[:8].isdigit():
        return None
    try:
        return datetime.strptime(rcept_no[:8], "%Y%m%d").date()
    except ValueError:
        return None
