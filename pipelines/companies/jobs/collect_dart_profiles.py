"""DART 기업개황 수집.

대표자·설립일·결산월·업종코드·홈페이지·주소를 채운다. 결산월(acc_mt)은 DART 재무의
회계연도 표기(fiscal_yymm)를 만들 때도 쓰이므로 재무 수집보다 먼저 돌아야 한다.

대상은 service_companies에 속한 법인이다. 상장사 전량이 아니다 — 1차 MVP가
반도체·2차전지라 그 밖은 채워도 화면에 나가지 않는다.
목록이 법인 단위여서 종목을 거치지 않고 corp_code로 바로 조회한다.
"""

from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.dart import get_dart_client
from pipelines.common.logging import get_logger
from pipelines.companies.extractors.dart import fetch_company_profile
from pipelines.companies.loaders.dart import (
    fetch_profile_targets,
    fetch_unresolved_universe,
    update_company_profile,
)

logger = get_logger(__name__)

CHUNK_SIZE = 50


def run(limit: int | None = None) -> None:
    """개요가 비어 있는 법인부터 기업개황을 채운다."""

    settings = get_settings()
    batch_size = limit or settings.dart_profile_batch_size

    client = get_dart_client()
    with session_scope() as session:
        targets = fetch_profile_targets(session, batch_size)
        unresolved = fetch_unresolved_universe(session)

    if unresolved:
        # 조인에서 조용히 빠지는 종목이라 로그로만 드러난다.
        logger.warning(
            "서비스 대상인데 수집 경로가 끊김: %d법인 %s",
            len(unresolved),
            [f"{name}({cid}) {reason}" for cid, name, reason in unresolved[:10]],
        )

    logger.info("DART 기업개황 수집 시작: 대상 %d법인", len(targets))

    updated = 0
    missing = 0
    failed: list[str] = []

    for corp_code in targets:
        try:
            profile = fetch_company_profile(corp_code, client=client)
        except Exception:
            logger.exception("기업개황 수집 실패: corp_code=%s", corp_code)
            failed.append(corp_code)
            continue

        if profile is None:
            missing += 1
            continue

        with session_scope() as session:
            updated += update_company_profile(session, profile)

    logger.info(
        "DART 기업개황 수집 완료: 갱신 %d건, 데이터 없음 %d건, 실패 %d건 %s",
        updated,
        missing,
        len(failed),
        failed[:10],
    )
