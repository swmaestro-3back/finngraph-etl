"""task: load_postgres — us.json + us_overview.json을 companies·stocks·aliases에 적재."""

from __future__ import annotations

import json
from pathlib import Path

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.loaders.us import load_us_companies
from pipelines.companies.transformers.us import build_us_companies

logger = get_logger(__name__)


def run(index_path: str, overview_path: str) -> int:
    index_rows = json.loads(Path(index_path).read_text(encoding="utf-8"))
    overviews = json.loads(Path(overview_path).read_text(encoding="utf-8"))
    companies = build_us_companies(index_rows, overviews)

    with session_scope() as session:
        result = load_us_companies(session, companies)

    logger.info(
        "US 상장사 적재 완료: 법인 %d, 종목 %d, 별칭 신규 %d (overview %d/%d)",
        result.company_count,
        result.stock_count,
        result.alias_count,
        len(overviews),
        len(index_rows),
    )
    return result.company_count
