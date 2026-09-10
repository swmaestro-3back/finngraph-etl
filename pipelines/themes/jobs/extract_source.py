"""
소스 크롤링 후 JSON 파일로 저장
다음 태스크를 위해 저장 경로 문자열 반환 (추후 S3 경로로 변환)
"""

from __future__ import annotations

import asyncio

from pipelines.common.clients.http import http_client
from pipelines.common.logging import get_logger
from pipelines.themes.extractors.factory import ExtractorFactory

logger = get_logger(__name__)


async def _run(source_name: str) -> str:
    async with http_client:
        logger.info("[%s] 추출 시작", source_name)
        extractor = ExtractorFactory.get_extractor(source_name)
        return str(await extractor.run())


def run(source_name: str) -> str:
    return asyncio.run(_run(source_name))
