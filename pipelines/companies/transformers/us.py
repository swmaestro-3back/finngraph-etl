"""인덱스 행(us.json) + 네이버 overview(us_overview.json) → UsCompany.

네이버 응답이 없는 종목은 인덱스 정보만으로 만든다. 로더가 COALESCE로 지난 값을
보존하므로 None을 그대로 내보내면 된다. reutersCode는 네이버 조회 키일 뿐 stocks의
표준코드가 아니다 — raw_attributes에만 남긴다.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from pipelines.companies.models import UsCompany

_BR = re.compile(r"<br\s*/?>", re.IGNORECASE)
# BRK.B → BRKb. 위키·한경은 점 표기, 네이버 reutersCode는 소문자 접미다(실측).
_CLASS_SUFFIX = re.compile(r"^([A-Z]+)\.([A-Z])$")

# 네이버 overview가 알려주는 거래소 이름. 그 외 값(ARCA 등)은 인덱스 시장을 그대로 쓴다.
MARKETS = ("NYSE", "NASDAQ")


def reuters_code_candidates(ticker: str, market: str) -> list[str]:
    """네이버 reutersCode 후보. 첫 응답이 오는 것이 실제 코드다.

    인덱스가 준 시장은 신뢰할 수 없다(S&P 500 표에는 거래소 열이 없어 전부 NYSE로 찍힌다).
    또 접미도 시장만으로 정해지지 않는다 — NYSE 종목 다수가 `.K`를 쓰고(실측: ABBV→409,
    ABBV.N→409, ABBV.K→200), NASDAQ인 KHC는 접미가 아예 없다(KHC→200, KHC.O/.K/.N→409).
    그래서 시장은 첫 후보의 순서만 정하고, 나머지 접미도 전부 훑는다.
    """

    code = _CLASS_SUFFIX.sub(lambda m: f"{m.group(1)}{m.group(2).lower()}", ticker)
    if market == "NASDAQ":
        return [f"{code}.O", code, f"{code}.K"]
    return [code, f"{code}.K", f"{code}.O"]


def _clean_summary(summary: str | None) -> str | None:
    if not summary:
        return None
    text = _BR.sub("\n", summary).strip()
    return text or None


def _clean_homepage(url: str | None) -> str | None:
    """네이버 url 끝의 '/'를 뗀다("https://www.apple.com/" → "https://www.apple.com")."""

    if not url:
        return None
    cleaned = url.strip().rstrip("/")
    return cleaned or None


def _join_address(summaries: dict[str, Any]) -> str | None:
    parts = [summaries.get("address"), summaries.get("city")]
    joined = ", ".join(p.strip() for p in parts if p and p.strip())
    return joined or None


def _fiscal_month(listed_info: dict[str, Any]) -> str | None:
    account_date = listed_info.get("accountDate")  # 'YYYY-MM-DD'
    return account_date[5:7] if account_date and len(account_date) >= 7 else None


def _listed_date(listed_info: dict[str, Any]) -> date | None:
    listed_at = listed_info.get("listedAt")  # '1980-12-12T16:00:00-05:00'
    return date.fromisoformat(listed_at[:10]) if listed_at else None


def build_us_companies(
    index_rows: list[dict[str, Any]], overviews: dict[str, dict[str, Any]]
) -> list[UsCompany]:
    companies: list[UsCompany] = []
    for row in index_rows:
        ticker, index_market = row["ticker"], row["market"]
        entry = overviews.get(ticker)
        overview = entry["overview"] if entry else None
        raw: dict[str, Any] = {
            "name_eng": row["name"],
            # 인덱스 크롤링이 찍은 시장. overview가 덮어쓰더라도 원본을 남긴다.
            "index_market": index_market,
        }
        listed_info = (overview or {}).get("stockItemListedInfo") or {}
        exchange = (listed_info.get("stockExchangeType") or {}).get("name")
        base = {
            "ticker": ticker,
            "market": exchange if exchange in MARKETS else index_market,
            "name": row["kr_name"],
            "name_eng": row["name"],
        }

        if overview is None:
            companies.append(UsCompany(**base, raw_attributes=raw))
            continue

        summaries = overview.get("summaries") or {}
        industry = overview.get("industry") or {}
        raw.update(
            {
                # 네이버가 실제로 응답한 reutersCode(AAPL.O, JPM.K, BRKb). stocks.standard_code에는
                # 넣지 않는다(US는 NULL) — 재조회 없이 네이버를 다시 부를 수 있도록 여기 남긴다.
                "reuters_code": entry["reuters_code"],
                "employees": summaries.get("employees"),
                "industry_group_kor": industry.get("industryGroupKor"),
                "naver_name": overview.get("companyName"),
                "naver_name_eng": overview.get("companyNameEng"),
            }
        )
        companies.append(
            UsCompany(
                **base,
                description=_clean_summary(overview.get("summary")),
                ceo_name=summaries.get("representativeName"),
                industry_code=industry.get("code"),
                homepage=_clean_homepage(summaries.get("url")),
                address=_join_address(summaries),
                fiscal_month=_fiscal_month(listed_info),
                listed_date=_listed_date(listed_info),
                listed_shares=listed_info.get("countOfListedStock"),
                raw_attributes=raw,
            )
        )
    return companies
