from __future__ import annotations

from typing import Any

from pipelines.common.http import http_client
from pipelines.common.logging import get_logger
from pipelines.common.neo4j import neo4j_database
from pipelines.themes.crud import delete_all_themes
from pipelines.themes.extractors.factory import ExtractorFactory
from pipelines.themes.loader import load
from pipelines.themes.models import Theme
from pipelines.themes.validator import validate

logger = get_logger(__name__)

SOURCES = ["judal", "naver", "antwinner"]


async def reset() -> None:
    """
    ETL 실행 전 기존 테마 엔티티 및 간선 삭제
    """
    async with neo4j_database:
        await delete_all_themes()
        logger.info("기존 Theme 및 연결 간선 삭제 완료")


async def extract_source(source_name: str) -> list[dict[str, Any]]:
    """
    SOURCES에서 테마 및 테마주 정보를 순차적으로 크롤링
    최종 크롤링 결과를 XCom으로 다음 Task에 전달
    """
    async with http_client:
        logger.info("[%s] 추출 시작", source_name)
        extractor = ExtractorFactory.get_extractor(source_name)
        themes = await extractor.extract()
        extractor.save(themes)
        return [theme.model_dump() for theme in themes]


async def validate_themes(
    raw_themes_nested: list[list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    저장된 XCom 데이터 자동 주입받아 실행
    테마 및 테마주에 대한 중복 제거 작업 진행
    """
    async with neo4j_database:
        all_themes = [Theme(**item) for sublist in raw_themes_nested for item in sublist]
        validated = await validate(all_themes)
        return [theme.model_dump() for theme in validated]


async def load_themes(validated_themes: list[dict[str, Any]]) -> None:
    """
    검증 완료된 테마를 Neo4j에 최종 적재
    """
    async with neo4j_database:
        await load([Theme(**item) for item in validated_themes])
        logger.info("전체 파이프라인 완료")
