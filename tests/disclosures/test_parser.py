"""공시 원문 파서 회귀 테스트.

fixtures/ 의 HTML·JSON 쌍은 프로토타입 파이프라인이 실제 DART 원문으로 만들어 검증한
골든 데이터다. 서식이 다른 세 경우를 고정한다 — KOSPI 일반, KOSDAQ 일반(해외 계약상대),
KOSDAQ 정정(정정신고 블록 포함).

계약상대 역매칭 키는 프로토타입(counterparty_stock_code)과 스키마가 달라
(counterparty_ticker/company_id) 골든 비교에서 빼고, build_disclosure 테스트에서
스텁 마스터로 따로 검증한다.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipelines.disclosures.models import CorpMasterRow
from pipelines.disclosures.transformers.counterparty import CorpMaster
from pipelines.disclosures.transformers.parser import (
    build_disclosure,
    market,
    parse_contract_fields,
)

FIXTURES = Path(__file__).parent / "fixtures"

# 프로토타입과 스키마가 달라진 역매칭 결과 키 — 골든 비교 대상에서 제외한다.
# (parse_contract_fields 단계에서는 승격 전이라 승격 항목도 아직 fields 안에 있다)
_RESOLUTION_KEYS = {
    "counterparty_corp_name",
    "counterparty_corp_code",
    "counterparty_stock_code",
    "counterparty_ticker",
    "counterparty_company_id",
    "counterparty_match_rule",
}

GOLDEN_RCEPT_NOS = ["20260821800662", "20260821900624", "20260821900482"]


def load_pair(rcept_no: str) -> tuple[str, dict]:
    html = (FIXTURES / f"{rcept_no}.html").read_text(encoding="utf-8")
    golden = json.loads((FIXTURES / f"{rcept_no}.json").read_text(encoding="utf-8"))
    return html, golden


@pytest.mark.parametrize("rcept_no", GOLDEN_RCEPT_NOS)
def test_parse_contract_fields_matches_golden(rcept_no: str) -> None:
    html, golden = load_pair(rcept_no)

    fields, is_correction = parse_contract_fields(html)

    assert is_correction == golden["is_correction"]
    expected = {k: v for k, v in golden["fields"].items() if k not in _RESOLUTION_KEYS}
    actual = {k: v for k, v in fields.items() if k not in _RESOLUTION_KEYS}
    assert actual == expected


def test_correction_block_parsed() -> None:
    html, golden = load_pair("20260821900482")

    fields, is_correction = parse_contract_fields(html)

    assert is_correction
    assert fields["correction"] == golden["fields"]["correction"]


def test_build_disclosure_resolves_filer_and_counterparty() -> None:
    html, golden = load_pair("20260821800662")
    filing = {
        "corp_code": "00103510",
        "corp_name": "인지컨트롤스",
        "stock_code": "023800",
        "corp_cls": "Y",
        "report_nm": "단일판매ㆍ공급계약체결",
        "rcept_no": "20260821800662",
        "flr_nm": "인지컨트롤스",
        "rcept_dt": "20260821",
    }
    corp_master = CorpMaster(
        [
            CorpMasterRow(company_id=1, corp_code="00103510", name="인지컨트롤스", ticker="023800"),
            CorpMasterRow(company_id=2, corp_code="00164788", name="현대모비스", ticker="012330"),
        ]
    )

    record = build_disclosure(filing, html, corp_master)

    assert record.rcept_no == "20260821800662"
    assert record.company_id == 1
    assert record.ticker == "023800"
    assert record.corp_cls == "KOSPI"
    assert not record.is_correction
    assert record.rcept_dt.isoformat() == "2026-08-21"
    assert record.link == golden["link"]
    # 승격 컬럼 — fields 에서 빠져 레코드 속성으로 옮겨진다.
    assert record.counterparty == "현대모비스"
    assert record.counterparty_corp_name == "현대모비스"
    assert record.counterparty_corp_code == "00164788"
    assert record.counterparty_ticker == "012330"
    assert record.contract_type == golden["fields"]["contract_type"]
    assert record.contract_name == golden["fields"]["contract_name"]
    assert record.start_date.isoformat() == "2028-09-01"
    assert record.end_date.isoformat() == "2035-12-31"
    assert record.order_date.isoformat() == "2026-08-20"
    for promoted in ("counterparty", "contract_type", "contract_name", "start_date"):
        assert promoted not in record.fields
    # 승격 대상이 아닌 역매칭 부가 정보는 fields 에 남는다.
    assert record.fields["counterparty_company_id"] == 2
    assert record.fields["counterparty_match_rule"] == "original:listed"
    assert record.meta["parser_version"] == 2


def test_build_disclosure_unknown_filer_keeps_nulls() -> None:
    html, _ = load_pair("20260821900624")
    filing = {
        "corp_code": "00999999",
        "corp_cls": "K",
        "report_nm": "단일판매ㆍ공급계약체결",
        "rcept_no": "20260821900624",
        "flr_nm": "성광벤드",
        "rcept_dt": "20260821",
    }

    record = build_disclosure(filing, html, CorpMaster([]))

    assert record.company_id is None
    assert record.ticker is None
    assert record.corp_cls == "KOSDAQ"
    # 해외 JV는 마스터에 없다 — 역매칭은 전부 None 이어야 한다.
    assert record.counterparty == "JGC Fluor BC LNG ll JV"
    assert record.counterparty_corp_code is None
    assert record.counterparty_ticker is None


def test_market_codes() -> None:
    assert market("Y") == "KOSPI"
    assert market("K") == "KOSDAQ"
    assert market("N") == "KONEX"
    assert market("E") == "UNLISTED"
    assert market(None) is None
