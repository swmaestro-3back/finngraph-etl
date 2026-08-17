"""task: extract_{source} — 소스 한 곳을 크롤링해 JSON으로 떨군다.

소스마다 별개 태스크로 도는 이유는 외부 웹이 이 파이프라인에서 가장 잘 깨지는 지점이라
한 소스의 실패가 나머지를 막으면 안 되기 때문이다. 반환값은 저장 경로 문자열이고
다음 태스크로 XCom을 통해 전달된다.
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
