"""DART 기업개황 수집.

대표자·설립일·결산월·업종코드·홈페이지·주소를 채운다. 결산월(acc_mt)은 DART 재무의
회계연도 표기(fiscal_yymm)를 만들 때도 쓰이므로 재무 수집보다 먼저 돌아야 한다.

대상은 service_companies에 속한 법인이다. 상장사 전량이 아니다 — 1차 MVP가
반도체·2차전지라 그 밖은 채워도 화면에 나가지 않는다.
목록이 법인 단위여서 종목을 거치지 않고 corp_code로 바로 조회한다.
"""

from __future__ import annotations

from pipelines.common.clients.dart import DartApiError, get_dart_client, is_quota_error
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.extractors.dart import fetch_company_profile
from pipelines.companies.loaders.dart import (
    fetch_profile_targets,
    fetch_unresolved_universe,
    update_company_profile,
)
from pipelines.companies.loaders.diagnostics import describe_universe

logger = get_logger(__name__)

CHUNK_SIZE = 50


def run(limit: int | None = None) -> None:
    """개요가 비어 있는 법인부터 기업개황을 채운다."""

    client = get_dart_client()
    with session_scope() as session:
        targets = fetch_profile_targets(session, limit)
        if not targets:
            logger.warning("DART 기업개황 수집 대상이 0건이다 — %s", describe_universe(session))
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

    quota_exceeded = False

    for corp_code in targets:
        try:
            profile = fetch_company_profile(corp_code, client=client)
        except DartApiError as exc:
            if is_quota_error(exc):
                # 남은 법인을 건너뛰면 안 된다. updated_at 이 갱신되지 않아야 다음 회차가
                # 여기서부터 이어받는다. 재시도해 봐야 소진된 한도에 다시 부딪힌다.
                logger.warning("DART 일 호출 한도 도달 — 여기까지 하고 다음 회차가 이어받는다")
                quota_exceeded = True
                break
            logger.exception("기업개황 수집 실패: corp_code=%s", corp_code)
            failed.append(corp_code)
            continue
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
        "DART 기업개황 수집 %s: 갱신 %d건, 데이터 없음 %d건, 실패 %d건 %s",
        "중단(한도)" if quota_exceeded else "완료",
        updated,
        missing,
        len(failed),
        failed[:10],
    )
