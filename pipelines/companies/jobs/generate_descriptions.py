"""기업 설명 생성.

정기공시 본문의 「사업의 개요」를 LLM으로 3~5문장 요약한다(description_source=DART_LLM).

법인당 DART 2회 + LLM 1회라 비싸다. 직전 원천의 접수번호를 목록 맨 앞과 대조해, 새
보고서가 올라온 법인만 다시 만든다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.dart import DartApiError, get_dart_client, is_quota_error
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.companies.extractors.dart import (
    fetch_document_text,
    fetch_periodic_report_receipts,
)
from pipelines.companies.loaders.descriptions import (
    fetch_description_targets,
    update_description,
)
from pipelines.companies.loaders.diagnostics import describe_universe
from pipelines.companies.transformers.description import (
    DESCRIPTION_SOURCE_DART_LLM,
    extract_business_section,
    summarize_business_section,
)

logger = get_logger(__name__)

# 정기공시 조회 구간. 제출이 늦는 법인을 감안해 넉넉히 본다.
REPORT_LOOKBACK_DAYS = 800

# 한 법인에서 시도할 공시 수. 첫 건이 본문을 못 주면 다음으로 내려간다.
# 3이면 최신 정정본 → 원공시 → 그 이전 회차까지 닿는다.
MAX_RECEIPT_ATTEMPTS = 3


def run(limit: int | None = None) -> None:
    """새 정기공시가 올라온 법인의 설명을 만든다."""

    client = get_dart_client()
    today = now_kst().date()
    start = today - timedelta(days=REPORT_LOOKBACK_DAYS)

    with session_scope() as session:
        targets = fetch_description_targets(session, limit)
        if not targets:
            logger.warning("기업 설명 수집 대상이 0건이다 — %s", describe_universe(session))

    logger.info("기업 설명 생성 시작: 후보 %d법인", len(targets))

    generated = 0
    unchanged = 0
    failed: list[str] = []

    quota_exceeded = False

    for company_id, name, corp_code, last_rcept_no in targets:
        try:
            receipts = fetch_periodic_report_receipts(corp_code, start, today, client=client)
        except DartApiError as exc:
            if is_quota_error(exc):
                logger.warning("DART 일 호출 한도 도달 — 여기까지 하고 다음 회차가 이어받는다")
                quota_exceeded = True
                break
            logger.exception("정기공시 목록 조회 실패: corp_code=%s name=%s", corp_code, name)
            failed.append(name)
            continue
        except Exception:
            logger.exception("정기공시 목록 조회 실패: corp_code=%s name=%s", corp_code, name)
            failed.append(name)
            continue

        latest = _latest_rcept_no(receipts)
        if not latest:
            logger.warning("정기공시 없음: corp_code=%s name=%s", corp_code, name)
            failed.append(name)
            continue

        if latest == last_rcept_no:
            unchanged += 1
            continue

        description = ""
        try:
            description = _summarize(name, receipts, client)
        except Exception:
            logger.exception("사업보고서 요약 실패: corp_code=%s name=%s", corp_code, name)

        # 실패하면 접수번호를 옮기지 않는다. 다음 회차에 다시 잡혀야 한다.
        if not description:
            failed.append(name)
            continue

        with session_scope() as session:
            update_description(
                session, company_id, description, DESCRIPTION_SOURCE_DART_LLM, latest
            )
        generated += 1

    logger.info(
        "기업 설명 생성 %s: %d건 생성, %d건 변경없음, %d건 실패 %s",
        "중단(한도)" if quota_exceeded else "완료",
        generated,
        unchanged,
        len(failed),
        failed[:10],
    )


def _latest_rcept_no(receipts: list[dict]) -> str:
    """목록 맨 앞(최신) 공시의 접수번호. 없으면 빈 문자열."""

    for receipt in receipts:
        rcept_no = str(receipt.get("rcept_no") or "").strip()
        if rcept_no:
            return rcept_no
    return ""


def _summarize(name: str, receipts: list[dict], client) -> str:
    """최신 정기공시부터 순서대로 시도해 사업 설명을 만든다.

    **한 건만 보고 포기하지 않는다.** 맨 앞 공시가 본문을 못 주거나(정정·서식 차이) 사업
    개요 절이 없을 수 있어서, 다음 공시로 내려가며 MAX_RECEIPT_ATTEMPTS회까지 시도한다.
    예전에는 receipts[0] 하나만 써서, 그 한 건이 실패하면 곧바로 업종 폴백으로 떨어졌다.

    Returns:
        str: 요약문. 시도한 공시에서 모두 발췌를 못 얻으면 빈 문자열.
    """

    for receipt in receipts[:MAX_RECEIPT_ATTEMPTS]:
        rcept_no = str(receipt.get("rcept_no") or "").strip()
        if not rcept_no:
            continue

        try:
            document = fetch_document_text(rcept_no, client=client)
        except Exception:
            # 본문이 없는 공시가 섞인다. 다음 것으로 넘어간다.
            logger.warning(
                "공시 본문 조회 실패, 다음 공시 시도: rcept_no=%s name=%s", rcept_no, name
            )
            continue

        excerpt = extract_business_section(document)
        if excerpt:
            return summarize_business_section(name, excerpt)

    return ""
