from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class CompanySyncResult:
    """국내 상장 기업 마스터 동기화 결과.

    법인 행을 만드는 단계가 아니므로 삽입 수를 세지 않는다. 세는 것은 상태 변화다.

    Attributes:
        listed_count (int): 상장으로 표시되거나 이름·시장이 갱신된 법인 수.
        delisted_count (int): 활성 종목 목록에서 사라져 상장폐지로 내린 법인 수.
        linked_count (int): stocks.company_id가 새로 연결되거나 바뀐 종목 수.
            이미 같은 법인을 가리키던 종목은 세지 않는다.
        alias_count (int): company_aliases에 새로 추가된 별칭 수.
            이미 있던 별칭은 ON CONFLICT DO NOTHING으로 빠지므로 세지 않는다.
    """

    listed_count: int
    delisted_count: int
    linked_count: int
    alias_count: int


@dataclass(frozen=True)
class CompanyFinancial:
    """기업 재무 한 기간치. 원천은 KIS와 DART 둘이다.

    **금액 단위는 전부 원(KRW)이다.** KIS는 억원으로 주고 DART는 원으로 주는데, 적재 전에
    원으로 맞춘다. 단위가 섞이면 두 원천을 한 화면에서 비교할 수 없다.

    손익은 **누적 기준**으로 통일한다. KIS 분기 행이 누적이고 DART의 당기 누계도 누적이라,
    분기 단독 값이 필요하면 파생 단계에서 차분으로 만든다.

    Attributes:
        source (str): 'KIS' | 'DART'.
        fs_div (str): 'CFS'(연결) | 'OFS'(별도). KIS는 연결만 제공한다.
        fiscal_yymm (str): 'YYYYMM' 결산 기준월.
        period_type (str): 'Q'(분기 누적) | 'A'(연간).
        disclosed_at (date | None): 공시일. DART 접수번호 앞 8자리에서 얻는다.
            KIS는 공시 시점을 주지 않아 None이며, 그래서 KIS 데이터로는
            point-in-time(look-ahead bias 회피)을 보장할 수 없다.
        rcept_no (str | None): DART 접수번호. 정정공시가 새 행이 되는 근거다.
    """

    company_id: int
    source: str
    fiscal_yymm: str
    period_type: str
    fs_div: str = "CFS"
    disclosed_at: date | None = None
    rcept_no: str | None = None
    revenue: int | None = None
    operating_income: int | None = None
    net_income: int | None = None
    total_assets: int | None = None
    total_liabilities: int | None = None
    total_equity: int | None = None
    roe: Decimal | None = None
    eps: Decimal | None = None
    bps: Decimal | None = None


@dataclass(frozen=True)
class CompanyProfile:
    """DART 기업개황.

    Attributes:
        corp_code (str): DART 고유번호 8자리. 이 값으로 companies 행을 찾는다.
        fiscal_month (str | None): 결산월 'MM'.
    """

    corp_code: str
    name: str
    name_eng: str | None = None
    ceo_name: str | None = None
    industry_code: str | None = None
    established_on: date | None = None
    fiscal_month: str | None = None
    homepage: str | None = None
    address: str | None = None


@dataclass(frozen=True)
class DartCorp:
    """corpCode.xml 한 행.

    Attributes:
        stock_code (str | None): KRX 단축코드. 상장 이력이 있는 법인만 갖는다.
            **상장사 매핑은 이 값으로 한다** — 법인명 매칭은 동명이인 때문에 위험하다.
        modify_date (date | None): DART 최종변경일.
    """

    corp_code: str
    name: str
    name_eng: str | None = None
    stock_code: str | None = None
    modify_date: date | None = None
