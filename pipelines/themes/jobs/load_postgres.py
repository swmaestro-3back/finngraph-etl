"""task: load_postgres — 병합 결과를 Postgres(themes/theme_stocks)에 증분 반영한다."""

from __future__ import annotations

import json
from pathlib import Path

from pipelines.common.logging import get_logger
from pipelines.themes.loaders.postgres import load_themes

logger = get_logger(__name__)


def run(merged_path: str) -> None:
    themes = json.loads(Path(merged_path).read_text(encoding="utf-8"))
    result = load_themes(themes)

    logger.info("테마 RDB 적재 결과: %s", result)
