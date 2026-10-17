"""네이버 증권 해외종목 overview API.

    https://stock.naver.com/api/securityService/stock/overview?reutersCode={code}

reutersCode는 티커·시장만으로 확정되지 않는다(NYSE 종목 다수가 `.K` 접미). 그래서
transformers.us.reuters_code_candidates가 준 후보를 순서대로 때려보고 처음 200이 오는
코드를 그 종목의 코드로 확정한다. 없는 코드는 409 Conflict(`Not Exist Master`)로 오므로
4xx는 재시도 없이 건너뛴다. 모든 후보가 빗나간 종목은 인덱스 정보만으로 적재되고,
로더가 COALESCE로 지난 값을 지킨다.

결과 형태: {ticker: {"reuters_code": 확정 코드, "overview": 응답 본문}}.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import aiohttp

from pipelines.common.clients.http import http_client
from pipelines.common.logging import get_logger
from pipelines.common.utils.retry import retry_external_call
from pipelines.companies.transformers.us import reuters_code_candidates

logger = get_logger(__name__)

OVERVIEW_URL = "https://stock.naver.com/api/securityService/stock/overview"
OVERVIEW_FILENAME = "us_overview.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://stock.naver.com/",
}
TIMEOUT = aiohttp.ClientTimeout(total=10)
CONCURRENCY = 5


class _RetryableHTTPError(Exception):
    """5xx·타임아웃만 재시도 대상으로 분리한다."""


@retry_external_call()
async def _get_json(code: str) -> dict[str, Any] | None:
    session = http_client.get_session()
    try:
        async with session.get(
            OVERVIEW_URL, params={"reutersCode": code}, headers=HEADERS, timeout=TIMEOUT
        ) as resp:
            if 400 <= resp.status < 500:
                logger.debug("네이버 overview 없음: %s (HTTP %d)", code, resp.status)
                return None
            if resp.status >= 500:
                raise _RetryableHTTPError(f"{code}: HTTP {resp.status}")
            return await resp.json()
    except (TimeoutError, aiohttp.ClientConnectionError) as exc:
        raise _RetryableHTTPError(f"{code}: {exc}") from exc


async def _fetch_one(code: str) -> dict[str, Any] | None:
    """한 코드의 응답. 어떤 예외도 그 코드 하나만 버리고 배치를 이어가게 삼킨다."""

    try:
        return await _get_json(code)
    except Exception as exc:  # noqa: BLE001 - 종목 하나의 실패가 배치를 끊으면 안 된다
        logger.error("네이버 overview 수집 실패: %s (%s)", code, exc)
        return None


async def _resolve(ticker: str, market: str) -> tuple[str | None, dict[str, Any] | None]:
    """후보 코드를 순서대로 시도해 처음 응답한 (코드, 본문)을 돌려준다."""

    for code in reuters_code_candidates(ticker, market):
        body = await _fetch_one(code)
        if body is not None:
            return code, body
    return None, None


async def fetch_overviews(index_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """티커별 {"reuters_code", "overview"}. 후보가 전부 빗나간 종목은 결과에 없다."""

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def _bounded(row: dict[str, Any]) -> tuple[str, str | None, dict[str, Any] | None]:
        async with semaphore:
            code, body = await _resolve(row["ticker"], row["market"])
        return row["ticker"], code, body

    triples = await asyncio.gather(*(_bounded(row) for row in index_rows))
    result = {
        ticker: {"reuters_code": code, "overview": body}
        for ticker, code, body in triples
        if body is not None
    }
    logger.info("네이버 overview %d/%d개 수집", len(result), len(index_rows))
    return result


async def crawl_us_overview(index_path: Path) -> Path:
    rows = json.loads(index_path.read_text(encoding="utf-8"))
    overviews = await fetch_overviews(rows)
    output = index_path.parent / OVERVIEW_FILENAME
    output.write_text(json.dumps(overviews, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("overview 저장: %s", output)
    return output
