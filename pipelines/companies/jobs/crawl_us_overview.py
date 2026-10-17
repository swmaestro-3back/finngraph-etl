"""task: crawl_overview — us.json의 티커마다 네이버 overview를 받아 us_overview.json으로 저장."""

from __future__ import annotations

import asyncio
from pathlib import Path

from pipelines.common.clients.http import http_client
from pipelines.companies.extractors.naver_overview import crawl_us_overview


async def _run(index_path: str) -> str:
    async with http_client:
        return str(await crawl_us_overview(Path(index_path)))


def run(index_path: str) -> str:
    return asyncio.run(_run(index_path))
