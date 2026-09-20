"""NASDAQ-100·S&P 500 편입 종목 크롤링 (Wikipedia 영문명 + 한경 한글명).

한글명이 필요한 이유: Neo4j Company 노드 키가 name(한글 정규명)이고, 뉴스 삼중항이
gazetteer 한글명으로 MERGE 하기 때문이다. 한경에 없는 종목은 한글명이 없어 버린다.
"""

from __future__ import annotations

import json
import re
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from pipelines.common.clients.http import http_client
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

# parents[1]은 `pipelines/companies`. .gitignore가 `pipelines/companies/data/*`를 고정한다.
DATA_ROOT = Path(__file__).parents[1] / "data"

# 한경: 일반 브라우저처럼 보이는 헤더.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}
# Wikimedia 로봇 정책(https://w.wiki/4wJS): aiohttp가 브라우저 UA를 흉내 내면 TLS 지문 불일치로
# 403이 난다(브라우저 헤더를 전부 붙여도 동일). 프로젝트명·연락처를 담은 서술형 UA만 통과한다.
WIKI_HEADERS = {
    "User-Agent": (
        "finngraph-etl/1.0 (https://github.com/swmaestro-3back/finngraph-etl; "
        "jaehongmin3627@gmail.com)"
    )
}
TIMEOUT = aiohttp.ClientTimeout(total=20)

WIKI_NASDAQ100_URL = "https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies"
WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HANKYUNG_SP500_URL = "https://www.hankyung.com/globalmarket/usa-stock-sp500"
HANKYUNG_NASDAQ100_URL = "https://www.hankyung.com/globalmarket/usa-stock-nasdaq100"

INDEX_FILENAME = "us.json"

DATE_FOLDER = re.compile(r"^\d{8}$")


def today_folder() -> Path:
    """`pipelines/companies/data/{YYYYMMDD}`를 만들고 반환한다."""

    folder = DATA_ROOT / date.today().strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def latest_data_folder(required: tuple[str, ...]) -> Path:
    """DATA_ROOT 아래 YYYYMMDD 폴더 중 required 파일을 모두 가진 가장 최신 폴더.

    크롤링 DAG와 적재 DAG가 분리돼 XCom으로 경로를 못 넘기므로 적재 쪽이 스스로 찾는다.
    두 파일이 같은 날짜 폴더에 묶여 있어 짝이 어긋나지 않는다.
    """

    candidates = sorted(
        (p for p in DATA_ROOT.iterdir() if p.is_dir() and DATE_FOLDER.match(p.name)),
        key=lambda p: p.name,
        reverse=True,
    )
    for folder in candidates:
        if all((folder / name).exists() for name in required):
            return folder

    raise FileNotFoundError(f"{DATA_ROOT} 아래에서 {required}를 모두 가진 날짜 폴더를 찾지 못했다.")


def parse_wikipedia_table(
    html: str, ticker_col: str, name_col: str, market: str, sp500: bool
) -> list[dict[str, Any]]:
    """페이지 첫 표에서 (티커, 영문명)을 뽑는다. 두 페이지가 열 이름만 다르다."""

    table = pd.read_html(StringIO(html))[0]
    rows = table[[ticker_col, name_col]].rename(columns={ticker_col: "ticker", name_col: "name"})
    return [
        {
            "ticker": str(r.ticker).strip(),
            "name": str(r.name).strip(),
            "market": market,
            "sp500": sp500,
        }
        for r in rows.itertuples(index=False)
    ]


def parse_hankyung(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    for item in soup.select("div.sub-section.usa-market table tbody tr a.stock-item"):
        name_div = item.find("div", class_="stock-name")
        symbol_div = item.find("div", class_="symbol")
        if name_div and symbol_div:
            rows.append(
                {
                    "kr_name": name_div.get_text(strip=True),
                    "ticker": symbol_div.get_text(strip=True),
                }
            )
    return rows


def merge_index_rows(
    nasdaq100: list[dict[str, Any]],
    sp500: list[dict[str, Any]],
    hankyung: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """NASDAQ 우선 병합 후 한경 한글명과 inner join.

    1. NASDAQ-100 종목이 S&P 500에도 있으면 sp500=True.
    2. 같은 티커면 NASDAQ 행을 남긴다(market='NASDAQ').
    3. 한경에 없는 티커는 버린다.
    """

    sp500_tickers = {row["ticker"] for row in sp500}
    merged: dict[str, dict[str, Any]] = {}
    for row in nasdaq100:
        merged[row["ticker"]] = {**row, "sp500": row["ticker"] in sp500_tickers}
    for row in sp500:
        merged.setdefault(row["ticker"], dict(row))

    kr_names: dict[str, str] = {}
    for row in hankyung:
        kr_names.setdefault(row["ticker"], row["kr_name"])

    result = []
    for ticker, row in merged.items():
        kr_name = kr_names.get(ticker)
        if kr_name is None:
            logger.warning("한경에 없어 제외: %s (%s)", ticker, row["name"])
            continue
        result.append(
            {
                "ticker": ticker,
                "name": row["name"],
                "kr_name": kr_name,
                "market": row["market"],
                "sp500": bool(row["sp500"]),
            }
        )
    return result


async def _get_text(url: str, headers: dict[str, str]) -> str:
    session = http_client.get_session()
    async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        return await resp.text()


async def crawl_us_index() -> Path:
    """네 페이지를 받아 병합하고 `data/{오늘}/us.json`에 저장한다."""

    nasdaq_html = await _get_text(WIKI_NASDAQ100_URL, WIKI_HEADERS)
    sp500_html = await _get_text(WIKI_SP500_URL, WIKI_HEADERS)
    hk_sp500_html = await _get_text(HANKYUNG_SP500_URL, BROWSER_HEADERS)
    hk_nasdaq_html = await _get_text(HANKYUNG_NASDAQ100_URL, BROWSER_HEADERS)

    rows = merge_index_rows(
        parse_wikipedia_table(nasdaq_html, "Ticker", "Company", "NASDAQ", False),
        parse_wikipedia_table(sp500_html, "Symbol", "Security", "NYSE", True),
        parse_hankyung(hk_sp500_html) + parse_hankyung(hk_nasdaq_html),
    )

    output = today_folder() / INDEX_FILENAME
    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("US 인덱스 %d개 저장: %s", len(rows), output)
    return output
