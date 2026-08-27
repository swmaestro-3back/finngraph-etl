"""서비스 대상 동기화.

theme_stocks 에 편입된 종목의 법인을 수집 대상으로 올린다. themes_pipeline 이 테마를
갱신한 직후에 돈다.

수집 대상이 곧 모든 파이프라인의 범위다 — 시세·수급·배당·재무·설명이 전부 이 목록을
읽는다. 그래서 편입만 하고 제외는 하지 않는다.
"""

from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.loaders.service import (
    count_service_companies,
    sync_service_companies,
)

logger = get_logger(__name__)


def run() -> None:
    """테마 편입 종목의 법인을 수집 대상에 더한다."""

    with session_scope() as session:
        added = sync_service_companies(session)
        total = count_service_companies(session)

    logger.info("서비스 대상 동기화 완료: 신규 %d법인, 총 %d법인", added, total)
