from __future__ import annotations

import asyncio

from pipelines.common.http import http_client
from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database
from pipelines.themes.extractors.factory import ExtractorFactory
from pipelines.themes.loader import load
from pipelines.themes.models import Theme
from pipelines.themes.validator import validate

logger = get_logger(__name__)

SOURCES = ["judal", "naver", "antwinner"]

async def _run_pipeline() -> None:
    """
    모든 SOURCE를 순차로 extract한 뒤, 모아진 데이터를 대상으로
    validate/load를 한 번씩만 수행한다.
    """
    all_themes: list[Theme] = []

    for source_name in SOURCES:
        logger.info("[%s] 추출 시작", source_name)
        extractor = ExtractorFactory.get_extractor(source_name)
        themes = await extractor.extract()
        extractor.save(themes)
        all_themes.extend(themes)

    validated_themes = await validate(all_themes)
    await load(validated_themes)

    logger.info("전체 파이프라인 완료")


async def _run_async() -> None:
    http_client.start()
    neo4j_database.init_driver()
    try:
        await _run_pipeline()
    finally:
        await http_client.stop()
        await neo4j_database.close()


def run() -> None:
    asyncio.run(_run_async())


if __name__ == "__main__":
    run()
