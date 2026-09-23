"""task: crawl_index — Wikipedia·한경에서 NASDAQ-100·S&P 500 편입 종목을 받아 us.json으로 저장.

다음 태스크를 위해 저장 경로 문자열을 반환한다(themes extract_source와 같은 방식).
"""

from __future__ import annotations

import asyncio

from pipelines.common.clients.http import http_client
from pipelines.companies.extractors.us_index import crawl_us_index


async def _run() -> str:
    async with http_client:
        return str(await crawl_us_index())


def run() -> str:
    return asyncio.run(_run())
