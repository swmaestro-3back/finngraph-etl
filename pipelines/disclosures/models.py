from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class CorpMasterRow:
    """companies 한 행. 계약상대 역매칭과 제출사 자기 식별에 함께 쓴다.

    Attributes:
        company_id (int): companies.id.
        corp_code (str): DART 고유번호(8자리).
        name (str): 법인명.
        ticker (str | None): 상장 종목 단축코드. 비상장이면 None.
    """

    company_id: int
    corp_code: str
    name: str
    ticker: str | None


@dataclass(frozen=True)
class SupplyContractEdge:
    """Neo4j SUPPLIES_TO 간선 한 개의 재료 — 계약상대 ticker 매칭에 성공한 공시만.

    Attributes:
        rcept_no (str): 접수번호. 간선 provenance 로 쌓인다.
        link (str): DART 뷰어 URL.
        filer_ticker (str): 제출사(공급자) 종목 단축코드.
        counterparty_ticker (str): 계약상대(수요자) 종목 단축코드.
    """

    rcept_no: str
    link: str
    filer_ticker: str
    counterparty_ticker: str


@dataclass(frozen=True)
class DisclosureRecord:
    """disclosures 한 행. fields/meta는 JSONB로 그대로 들어간다.

    Attributes:
        rcept_no (str): 접수번호. 테이블의 유일키이자 수집 재개 상태다.
        corp_code (str): 제출사 DART 고유번호.
        company_id (int | None): 제출사 companies.id. corp_code 동기화 전이면 None.
        ticker (str | None): 제출사 종목 단축코드. 비상장이면 None.
        corp_cls (str | None): 'KOSPI' | 'KOSDAQ' | 'KONEX' | 'UNLISTED'.
        report_nm (str | None): 보고서명. 정정이면 '[기재정정]' 류 접두가 붙는다.
        is_correction (bool): 원문에 정정신고 블록이 있는지.
        rcept_dt (date): 접수일자.
        flr_nm (str | None): 공시 제출인명.
        link (str): DART 뷰어 URL.
        contract_type (str | None): 판매ㆍ공급계약 구분.
        contract_name (str | None): 체결계약명.
        counterparty (str | None): 계약상대 원문 표기(자유 텍스트).
        counterparty_corp_name (str | None): 역매칭된 법인명.
        counterparty_corp_code (str | None): 역매칭된 DART 고유번호.
        counterparty_ticker (str | None): 역매칭된 종목 단축코드(상장사만).
        start_date (date | None): 계약 시작일. 비정형 기재는 None(원문은 fields 에 남음).
        end_date (date | None): 계약 종료일. 위와 같다.
        order_date (date | None): 계약(수주)일자. 위와 같다.
        fields (dict[str, Any]): 승격되지 않은 나머지 계약 내용(금액류ㆍ조건ㆍ정정 블록 등).
        meta (dict[str, Any]): fetched_at, parser_version.
    """

    rcept_no: str
    corp_code: str
    company_id: int | None
    ticker: str | None
    corp_cls: str | None
    report_nm: str | None
    is_correction: bool
    rcept_dt: date
    flr_nm: str | None
    link: str
    contract_type: str | None
    contract_name: str | None
    counterparty: str | None
    counterparty_corp_name: str | None
    counterparty_corp_code: str | None
    counterparty_ticker: str | None
    start_date: date | None
    end_date: date | None
    order_date: date | None
    fields: dict[str, Any]
    meta: dict[str, Any]
