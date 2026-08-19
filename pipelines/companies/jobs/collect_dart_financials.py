"""DART 재무 수집.

KIS가 못 주는 두 가지를 채운다.
- **비상장 법인 재무** — 시드 목록 대상.
- **상장사 별도(OFS) 재무** — KIS는 연결만 준다. 프론트의 separateAssets가 여기서 나온다.

대상은 개요 수집과 같다 — service_companies에 속한 법인이다.
상장사 전량이 아니다.

사업보고서(연간)만 받는다. 분기·반기는 KIS 쪽이 상장사 전체를 이미 분기 단위로 주고,
비상장 분기 재무는 화면 요구사항에 없다.

공시일(disclosed_at)이 접수번호에서 나오므로 이 데이터는 point-in-time을 만족한다.
KIS 재무와 달리 "그 시점에 시장이 알고 있었는가"를 따질 수 있다.
"""

from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.config import get_settings
from pipelines.common.dart import get_dart_client
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.companies.extractors.dart import (
    FS_DIV_CONSOLIDATED,
    FS_DIV_SEPARATE,
    fetch_financial_statements,
)
from pipelines.companies.loaders.dart import fetch_dart_financial_targets, fetch_unresolved_universe
from pipelines.companies.loaders.financials import upsert_financials
from pipelines.companies.models import CompanyFinancial
from pipelines.companies.transformers.dart import build_dart_financials

logger = get_logger(__name__)

FS_DIVS = (FS_DIV_CONSOLIDATED, FS_DIV_SEPARATE)


def run(limit: int | None = None, years: int | None = None) -> None:
    """갱신이 오래된 법인부터 DART 연간 재무를 수집한다.

    Args:
        limit (int | None): 이번 회차 처리 법인 수.
        years (int | None): 받아올 사업연도 개수.
    """

    settings = get_settings()
    batch_size = limit or settings.dart_financial_batch_size
    year_count = years or settings.dart_financial_years

    # 사업보고서는 결산 후 3개월쯤 뒤에 나온다. 올해분은 아직 없을 수 있어 작년부터 센다.
    latest_year = now_kst().year - 1
    business_years = [str(latest_year - offset) for offset in range(year_count)]

    client = get_dart_client()
    with session_scope() as session:
        targets = fetch_dart_financial_targets(session, batch_size)
        unresolved = fetch_unresolved_universe(session)

    if unresolved:
        # 조인에서 조용히 빠지는 종목이라 로그로만 드러난다.
        logger.warning(
            "서비스 대상인데 수집 경로가 끊김: %d법인 %s",
            len(unresolved),
            [f"{name}({cid}) {reason}" for cid, name, reason in unresolved[:10]],
        )

    logger.info(
        "DART 재무 수집 시작: 대상 %d법인 × %d개 연도 × %d개 구분",
        len(targets),
        len(business_years),
        len(FS_DIVS),
    )

    total_rows = 0
    failed: list[str] = []

    for company_id, corp_code, fiscal_month in targets:
        financials: list[CompanyFinancial] = []
        for business_year in business_years:
            for fs_div in FS_DIVS:
                try:
                    rows = fetch_financial_statements(
                        corp_code,
                        business_year,
                        fs_div,
                        client=client,
                    )
                except Exception:
                    logger.exception(
                        "DART 재무 수집 실패: corp_code=%s year=%s fs_div=%s",
                        corp_code,
                        business_year,
                        fs_div,
                    )
                    failed.append(f"{corp_code}:{business_year}:{fs_div}")
                    continue

                financials.extend(build_dart_financials(company_id, rows, fs_div, fiscal_month))

        if financials:
            with session_scope() as session:
                total_rows += upsert_financials(session, financials)

    logger.info(
        "DART 재무 수집 완료: %d행 적재, 실패 %d건 %s", total_rows, len(failed), failed[:10]
    )
