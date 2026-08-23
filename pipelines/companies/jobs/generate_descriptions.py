"""기업 설명 생성.

정기공시 본문의 「사업의 개요」를 LLM으로 1~2문장 요약한다(description_source=DART_LLM).

**업종·주요제품 조합 폴백은 두지 않는다.** 예전에는 KRX 목록으로 폴백 문장을 만들었는데,
정기공시를 사업·반기·분기 전부 최신순으로 보고 본문 없는 [첨부정정]을 걸러내자 361법인이
모두 본문 경로로 채워졌다. 폴백이 실제로 기여하는 곳이 0이 되었고, 비상장은 KRX 목록에
없어 애초에 폴백이 닿지 않았다.

국내 상장사에 위키데이터를 쓰지 않는 이유는 커버리지다. 시가총액 2000위권의 한국어 설명
보유율이 0%이고, 있어도 "company in Seoul, South Korea" 수준이다.

법인당 DART 2회(목록·원문) + LLM 1회라 비싸다. 연 1회 갱신이면 충분하고, 회차마다
배치 크기만큼만 처리한다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.dart import get_dart_client
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
    """설명이 없는 법인부터 설명을 만든다."""

    settings = get_settings()
    batch_size = limit or settings.company_description_batch_size

    client = get_dart_client()
    today = now_kst().date()
    start = today - timedelta(days=REPORT_LOOKBACK_DAYS)

    with session_scope() as session:
        targets = fetch_description_targets(session, batch_size)

    logger.info("기업 설명 생성 시작: 대상 %d법인", len(targets))

    generated = 0
    skipped: list[str] = []

    for company_id, name, corp_code in targets:
        description = ""
        try:
            description = _summarize_latest_report(name, corp_code, start, today, client)
        except Exception:
            logger.exception("사업보고서 요약 실패: corp_code=%s name=%s", corp_code, name)

        if not description:
            skipped.append(name)
            continue

        with session_scope() as session:
            update_description(session, company_id, description, DESCRIPTION_SOURCE_DART_LLM)
        generated += 1

    logger.info(
        "기업 설명 생성 완료: %d건 생성, %d건 실패 %s",
        generated,
        len(skipped),
        skipped[:10],
    )


def _summarize_latest_report(name, corp_code, start, end, client) -> str:
    """최신 정기공시부터 순서대로 시도해 사업 설명을 만든다.

    **한 건만 보고 포기하지 않는다.** 맨 앞 공시가 본문을 못 주거나(정정·서식 차이) 사업
    개요 절이 없을 수 있어서, 다음 공시로 내려가며 MAX_RECEIPT_ATTEMPTS회까지 시도한다.
    예전에는 receipts[0] 하나만 써서, 그 한 건이 실패하면 곧바로 업종 폴백으로 떨어졌다.

    Returns:
        str: 요약문. 시도한 공시에서 모두 발췌를 못 얻으면 빈 문자열.
    """

    receipts = fetch_periodic_report_receipts(corp_code, start, end, client=client)

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
