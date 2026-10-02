from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

from sqlalchemy.orm import Session

from pipelines.common.clients.dart import DartClient, QuotaExceeded, get_dart_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.companies.loaders.dart import update_company_profile
from pipelines.companies.loaders.descriptions import update_description
from pipelines.companies.transformers.description import (
    DESCRIPTION_SOURCE_DART_LLM,
    extract_business_section,
    summarize_business_section,
)
from pipelines.market_calendar.extractors.dart_offerings import (
    fetch_document,
    fetch_ipo_filing_rows,
    fetch_offering_groups,
    fetch_profile,
)
from pipelines.market_calendar.loaders.offerings import (
    mark_description_ready,
    select_company_refs,
    select_filing_states,
    select_ksd_offering_refs,
    select_link_targets,
    select_listed_tickers,
    update_filing_terms,
    update_filing_tickers,
    upsert_filing_head,
)
from pipelines.market_calendar.models import CompanyRef, IpoFilingHead, StoredFiling
from pipelines.market_calendar.transformers.offerings import (
    derive_head,
    group_filing_rows,
    link_tickers,
    merge_stored,
    needs_description,
    needs_profile,
    needs_terms,
    terms_current,
    to_offering_terms,
)

logger = get_logger(__name__)

LOOKBACK_DAYS = 89
DEFAULT_DESCRIPTION_LIMIT = 10


def run(
    today: date | None = None,
    client: DartClient | None = None,
    description_limit: int | None = DEFAULT_DESCRIPTION_LIMIT,
) -> int:
    today = today or now_kst().date()
    client = client or get_dart_client()
    stats: Counter[str] = Counter()

    try:
        rows = fetch_ipo_filing_rows(client, today - timedelta(days=LOOKBACK_DAYS), today)
    except QuotaExceeded:
        logger.warning("DART 신고서: 목록 조회가 일 한도에 걸려 다음 실행에서 이어간다")
        return 0

    heads = [head for head in map(derive_head, group_filing_rows(rows).values()) if head]
    codes = [head.corp_code for head in heads]
    with session_scope() as session:
        stored = select_filing_states(session, codes)
        companies = select_company_refs(session, codes)

    quota = False
    present: list[IpoFilingHead] = []
    try:
        for head in heads:
            previous = stored.get(head.corp_code)
            merged = merge_stored(head, previous)
            if _sync_filing(merged, previous, today, client, stats):
                present.append(merged)
                if needs_profile(merged, companies.get(head.corp_code)):
                    _sync_profile(merged, client, stats)
        _describe(present, stored, companies, client, description_limit, stats)
    except QuotaExceeded:
        quota = True
        logger.warning("DART 신고서: 일 한도에 걸려 여기까지 적재하고 다음 실행에서 이어간다")

    with session_scope() as session:
        stats["linked"] = _link(session)

    logger.info(
        "DART 신고서 %s: 회사 %d, 머리 적재 %d, 약관 갱신 %d, 약관 대기 %d, 약관 실패 %d, "
        "개황 %d, 소개 %d(LLM %d, 발췌 없음 %d, 기존 설명 %d), 종목 연결 %d",
        "중단(한도)" if quota else "완료",
        len(heads),
        stats["heads"],
        stats["terms"],
        stats["terms_pending"],
        stats["terms_failed"],
        stats["profiles"],
        stats["described"],
        stats["llm"],
        stats["no_excerpt"],
        stats["existing"],
        stats["linked"],
    )
    return stats["heads"]


def _sync_filing(
    head: IpoFilingHead,
    previous: StoredFiling | None,
    today: date,
    client: DartClient,
    stats: Counter[str],
) -> bool:
    terms = None
    if needs_terms(head, previous):
        try:
            groups = fetch_offering_groups(client, head.corp_code, head.first_filed_on, today)
        except QuotaExceeded:
            raise
        except Exception:
            logger.exception(
                "DART 신고서: 약관 조회 실패 corp_code=%s name=%s", head.corp_code, head.corp_name
            )
            stats["terms_failed"] += 1
            return previous is not None
        if terms_current(head, groups):
            terms = to_offering_terms(groups)
            stats["terms"] += 1
        else:
            stats["terms_pending"] += 1
            if previous is not None:
                return True
    with session_scope() as session:
        upsert_filing_head(session, head)
        if terms is not None:
            update_filing_terms(session, head.corp_code, terms)
    stats["heads"] += 1
    return True


def _sync_profile(head: IpoFilingHead, client: DartClient, stats: Counter[str]) -> None:
    try:
        profile = fetch_profile(client, head.corp_code)
    except QuotaExceeded:
        raise
    except Exception:
        logger.exception("DART 신고서: 기업개황 조회 실패 corp_code=%s", head.corp_code)
        return
    if profile is None:
        return
    with session_scope() as session:
        update_company_profile(session, profile)
    stats["profiles"] += 1


def _describe(
    heads: list[IpoFilingHead],
    stored: dict[str, StoredFiling],
    companies: dict[str, CompanyRef],
    client: DartClient,
    limit: int | None,
    stats: Counter[str],
) -> None:
    for head in heads:
        if limit is not None and stats["llm"] >= limit:
            return
        company = companies.get(head.corp_code)
        if company is None or not needs_description(head, stored.get(head.corp_code), company):
            continue
        if company.has_description:
            with session_scope() as session:
                mark_description_ready(session, head.corp_code)
            stats["existing"] += 1
            continue
        excerpt = _excerpt(head, client)
        if not excerpt:
            stats["no_excerpt"] += 1
            continue
        stats["llm"] += 1
        try:
            summary = summarize_business_section(head.corp_name, excerpt)
        except Exception:
            logger.exception("DART 신고서: 소개 요약 실패 corp_code=%s", head.corp_code)
            continue
        if not summary:
            continue
        with session_scope() as session:
            update_description(
                session,
                company.company_id,
                summary,
                DESCRIPTION_SOURCE_DART_LLM,
                head.document_rcept_no or head.latest_rcept_no,
            )
            mark_description_ready(session, head.corp_code)
        stats["described"] += 1


def _excerpt(head: IpoFilingHead, client: DartClient) -> str:
    try:
        document = fetch_document(client, head.document_rcept_no or head.latest_rcept_no)
        return extract_business_section(document)
    except QuotaExceeded:
        raise
    except Exception:
        logger.exception(
            "DART 신고서: 원문 조회 실패 corp_code=%s rcept_no=%s",
            head.corp_code,
            head.document_rcept_no,
        )
        return ""


def _link(session: Session) -> int:
    targets = select_link_targets(session)
    changes = link_tickers(
        targets,
        select_ksd_offering_refs(session),
        select_listed_tickers(session, [target.corp_code for target in targets]),
    )
    return update_filing_tickers(session, changes)
