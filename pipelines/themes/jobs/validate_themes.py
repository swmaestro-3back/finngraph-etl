"""task: validate_themes — 소스별 JSON을 모아 검증·병합하고 하나로 합친다.

소스 extract 태스크들이 **전부** 끝나야 시작할 수 있는 join 지점이다. 중복 테마 판정이
전체 소스를 함께 봐야 성립하기 때문이다.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date
from pathlib import Path

from pipelines.common.clients.neo4j import neo4j_database
from pipelines.common.logging import get_logger
from pipelines.themes.models import Theme
from pipelines.themes.transformers.validator import validate

logger = get_logger(__name__)

# parents[1]은 `pipelines/themes` — 이 파일이 jobs/ 아래에 있어 한 단계 올라간다.
# extractor가 쓰는 폴더와 같아야 한다(같은 실행의 산출물이 한 폴더에 모인다).
DATA_ROOT = Path(__file__).parents[1] / "data"


def today_folder() -> Path:
    """`pipelines/themes/data/{YYYYMMDD}`를 만들고 반환한다."""

    folder = DATA_ROOT / date.today().strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder


async def _run(source_paths: list[str]) -> str:
    async with neo4j_database:
        themes: list[Theme] = []
        for path in source_paths:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
            themes.extend(Theme(**item) for item in raw)

        validated = await validate(themes)

        output = today_folder() / "validated.json"
        output.write_text(
            json.dumps([theme.model_dump() for theme in validated], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("검증 완료 %d개 테마 저장: %s", len(validated), output)
        return str(output)


def run(source_paths: list[str]) -> str:
    return asyncio.run(_run(source_paths))
