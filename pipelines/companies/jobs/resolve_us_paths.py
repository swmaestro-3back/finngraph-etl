"""task: resolve_paths — 최신 크롤링 산출물 폴더에서 us.json·us_overview.json 경로를 찾는다."""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.companies.extractors.naver_overview import OVERVIEW_FILENAME
from pipelines.companies.extractors.us_index import INDEX_FILENAME, latest_data_folder

logger = get_logger(__name__)


def run() -> dict[str, str]:
    folder = latest_data_folder((INDEX_FILENAME, OVERVIEW_FILENAME))
    logger.info("최신 크롤링 산출물 폴더: %s", folder)
    return {
        "index_path": str(folder / INDEX_FILENAME),
        "overview_path": str(folder / OVERVIEW_FILENAME),
    }
