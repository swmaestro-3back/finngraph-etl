from __future__ import annotations

import json
from datetime import date
from pathlib import Path

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

# extractor.save()와 동일한 루트(pipelines/themes/data)를 공유한다.
_DATA_ROOT = Path(__file__).parents[1] / "data"


def _today_folder() -> Path:
    """오늘 날짜 폴더(pipelines/themes/data/{YYYYMMDD})를 만들고 반환한다."""
    folder = _DATA_ROOT / date.today().strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


async def reset() -> None:
    """
    ETL 실행 전 기존 테마 엔티티 및 간선 삭제
    """
    async with neo4j_database:
        await delete_all_themes()
        logger.info("기존 Theme 및 연결 간선 삭제 완료")


async def extract_source(source_name: str) -> str:
    """
    SOURCES에 해당하는 웹사이트의 테마 및 테마주 정보를 크롤링해 JSON 파일로 저장
    저장경로만 리턴해 다음 XCom을 통해 다음 Task로 전달
    """
    async with http_client:
        logger.info("[%s] 추출 시작", source_name)
        extractor = ExtractorFactory.get_extractor(source_name)
        themes = await extractor.extract()
        output = extractor.save(themes)
        return str(output)


async def validate_themes(paths: list[str]) -> str:
    """
    extract가 저장한 소스별 JSON 파일 경로들을 받아 읽어 테마주 중복 제거 및 병합
    extract와 동일하게 JSON 파일로 저장 후 경로만 리턴하여 다음 Task로 전달
    """
    async with neo4j_database:
        all_themes: list[Theme] = []
        for path in paths:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            all_themes.extend(Theme(**item) for item in raw)

        validated = await validate(all_themes)

        output = _today_folder() / "validated.json"
        output.write_text(
            json.dumps([theme.model_dump() for theme in validated], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("검증 완료 %d개 테마 저장: %s", len(validated), output)
        return str(output)


async def load_themes(validated_path: str) -> None:
    """
    validate가 저장한 validated.json 경로를 받아 읽어 Neo4j에 최종 적재
    """
    async with neo4j_database:
        raw = json.loads(Path(validated_path).read_text(encoding="utf-8"))
        await load([Theme(**item) for item in raw])
        logger.info("전체 파이프라인 완료")
