"""task: merge_themes — 소스별 추출 결과를 하나의 테마 목록으로 합친다.

종목 검증 → 배치 내 중복 테마 합집합 → 기존 DB 테마와 이름 맞추기 순서다. DB 조회는 여기서만
하고 transformers 에는 결과만 넘긴다. 산출물은 extract 와 같은 날짜 폴더의 merged.json 이다.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipelines.common.logging import get_logger
from pipelines.themes.extractors.base import today_folder
from pipelines.themes.models import Theme
from pipelines.themes.references.postgres import (
    fetch_existing_themes,
    fetch_stock_names_by_tickers,
)
from pipelines.themes.transformers.company_filter import filter_companies
from pipelines.themes.transformers.merger import merge_batch, merge_existing

logger = get_logger(__name__)


def run(source_paths: list[str]) -> str:
    themes: list[Theme] = []
    for path in source_paths:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        themes.extend(Theme(**item) for item in raw)

    tickers = sorted({c.ticker for theme in themes for c in theme.companies})
    themes = filter_companies(themes, fetch_stock_names_by_tickers(tickers))
    themes = merge_batch(themes)
    themes = merge_existing(themes, fetch_existing_themes())

    output = today_folder() / "merged.json"
    output.write_text(
        json.dumps([theme.model_dump() for theme in themes], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("병합 완료 %d개 테마 저장: %s", len(themes), output)
    return str(output)
