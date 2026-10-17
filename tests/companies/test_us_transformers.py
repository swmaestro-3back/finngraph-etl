from __future__ import annotations

from datetime import date

import pytest

from pipelines.companies.transformers.us import build_us_companies, reuters_code_candidates


@pytest.mark.parametrize(
    ("ticker", "market", "expected"),
    [
        ("AAPL", "NASDAQ", ["AAPL.O", "AAPL", "AAPL.K"]),
        ("JPM", "NYSE", ["JPM", "JPM.K", "JPM.O"]),
        ("BRK.B", "NYSE", ["BRKb", "BRKb.K", "BRKb.O"]),
    ],
)
def test_reuters_code_candidates(ticker: str, market: str, expected: list[str]) -> None:
    assert reuters_code_candidates(ticker, market) == expected


INDEX_ROW = {
    "ticker": "AAPL",
    "name": "Apple Inc.",
    "kr_name": "애플",
    "market": "NASDAQ",
    "sp500": True,
}

OVERVIEW = {
    "companyName": "애플",
    "companyNameEng": "Apple Inc.",
    "summary": "애플은 스마트폰을 판매한다.<br>제품은 iPhone이다.<br><br>서비스도 있다.",
    "summaries": {
        "representativeName": "Timothy D. Cook",
        "employees": 166000,
        "city": "CUPERTINO, CA",
        "address": "One Apple Park Way",
        "url": "https://www.apple.com/",
    },
    "industry": {"code": "57106020", "industryGroupKor": "전화 및 소형 장치"},
    "stockItemListedInfo": {
        "stockExchangeType": {"name": "NASDAQ"},
        "accountDate": "2025-09-27",
        "listedAt": "1980-12-12T16:00:00-05:00",
        "countOfListedStock": 14594180000,
    },
}


ENTRY = {"reuters_code": "AAPL.O", "overview": OVERVIEW}


def test_build_us_companies_with_overview() -> None:
    [company] = build_us_companies([INDEX_ROW], {"AAPL": ENTRY})

    assert company.ticker == "AAPL"
    assert company.market == "NASDAQ"
    assert company.name == "애플"
    assert company.name_eng == "Apple Inc."
    assert not hasattr(company, "standard_code")  # US는 stocks.standard_code가 NULL
    assert (
        company.description == "애플은 스마트폰을 판매한다.\n제품은 iPhone이다.\n\n서비스도 있다."
    )
    assert company.ceo_name == "Timothy D. Cook"
    assert company.industry_code == "57106020"
    assert company.homepage == "https://www.apple.com"  # 끝의 '/' 제거
    assert company.address == "One Apple Park Way, CUPERTINO, CA"
    assert company.fiscal_month == "09"
    assert company.listed_date == date(1980, 12, 12)
    assert company.listed_shares == 14594180000
    assert company.raw_attributes == {
        "name_eng": "Apple Inc.",
        "index_market": "NASDAQ",
        "reuters_code": "AAPL.O",
        "employees": 166000,
        "industry_group_kor": "전화 및 소형 장치",
        "naver_name": "애플",
        "naver_name_eng": "Apple Inc.",
    }


def test_build_us_companies_without_overview_keeps_index_fields_only() -> None:
    [company] = build_us_companies([INDEX_ROW], {})

    assert company.homepage is None
    assert company.description is None
    assert company.ceo_name is None
    assert company.listed_date is None
    assert company.raw_attributes == {
        "name_eng": "Apple Inc.",
        "index_market": "NASDAQ",
    }


def test_build_us_companies_address_uses_only_present_parts() -> None:
    overview = {**OVERVIEW, "summaries": {**OVERVIEW["summaries"], "city": None}}

    [company] = build_us_companies([INDEX_ROW], {"AAPL": {**ENTRY, "overview": overview}})

    assert company.address == "One Apple Park Way"


def test_build_us_companies_overview_exchange_overrides_index_market() -> None:
    """S&P 500 표에는 거래소 열이 없어 전부 NYSE로 찍힌다 — overview가 진실이다."""

    row = {**INDEX_ROW, "ticker": "AKAM", "market": "NYSE"}
    entry = {"reuters_code": "AKAM.O", "overview": OVERVIEW}

    [company] = build_us_companies([row], {"AKAM": entry})

    assert company.market == "NASDAQ"
    assert company.raw_attributes["reuters_code"] == "AKAM.O"
    assert company.raw_attributes["index_market"] == "NYSE"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://www.apple.com/", "https://www.apple.com"),
        ("https://abc.xyz", "https://abc.xyz"),
        ("https://www.amd.com//", "https://www.amd.com"),
        ("", None),
        (None, None),
    ],
)
def test_build_us_companies_strips_trailing_slash_from_homepage(
    url: str | None, expected: str | None
) -> None:
    overview = {**OVERVIEW, "summaries": {**OVERVIEW["summaries"], "url": url}}

    [company] = build_us_companies([INDEX_ROW], {"AAPL": {**ENTRY, "overview": overview}})

    assert company.homepage == expected
