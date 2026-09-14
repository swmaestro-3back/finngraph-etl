"""search_history 테이블 읽기·쓰기.

읽기: 인자로 받은 테마의 편입 종목을 기업 단위로 모으고, search_history 의 마지막 검색
시각이 재검색 간격을 넘긴 기업만 CompanyQuery 로 만든다 — news_scheduled_pipeline 의 쿼리
원천이다. 쓰기: 검색을 마친 기업의 last_searched_at 을 갱신한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 테마 편입 종목과 그 기업의 마지막 검색 시각. company_id 가 NULL 인 종목도 함께 읽어
# 호출자가 세고 경고한다. 같은 기업의 종목이 여럿이면 stock id 순으로 첫 종목명을 쓴다.
SELECT_THEME_COMPANIES_SQL = text(
    """
    SELECT s.company_id, s.name, sh.last_searched_at
      FROM theme_stocks ts
      JOIN stocks s ON s.id = ts.stock_id AND s.is_active
      LEFT JOIN search_history sh ON sh.company_id = s.company_id
     WHERE ts.theme_id = ANY(:theme_ids)
     ORDER BY s.company_id NULLS LAST, s.id;
    """
)

# 검색을 마친 기업의 마지막 검색 시각 upsert
UPSERT_SEARCH_HISTORY_SQL = text(
    """
    INSERT INTO search_history (company_id, last_searched_at)
    SELECT unnest(CAST(:ids AS BIGINT[])), :searched_at
    ON CONFLICT (company_id) DO UPDATE
       SET last_searched_at = EXCLUDED.last_searched_at;
    """
)


@dataclass(frozen=True)
class CompanyQuery:
    company_id: int
    name: str
    # search_history.last_searched_at. None 이면 검색한 적이 없어 lookback 전체를 읽는다.
    watermark: datetime | None = None


@dataclass(frozen=True)
class CompanyQueryBatch:
    theme_ids: list[int]
    queries: list[CompanyQuery]
    skipped_no_company: int
    skipped_not_due: int


def group_company_rows(
    theme_ids: list[int],
    rows: list[tuple[int | None, str, datetime | None]],
    interval_hours: int,
    now: datetime,
) -> CompanyQueryBatch:
    """(company_id, name, last_searched_at) 행을 기업 단위 쿼리로 합친다.

    같은 기업이 여러 테마·여러 종목으로 나오면 쿼리 하나로 합치고 첫 행의 종목명을 쓴다.
    last_searched_at 이 `now - interval_hours` 보다 늦으면 아직 재검색 시점이 아니라 제외한다.
    company_id 가 NULL 인 종목은 news_companies 에 연결할 수 없어 경고 후 제외한다.
    """

    due_before = now - timedelta(hours=interval_hours)
    by_company: dict[int, CompanyQuery] = {}
    skipped_names: set[str] = set()
    skipped_not_due: set[int] = set()

    for company_id, name, last_searched_at in rows:
        if company_id is None:
            if name not in skipped_names:
                skipped_names.add(name)
                logging.warning("company_id 없는 종목 제외: name=%s", name)
            continue

        company_id = int(company_id)
        if company_id in by_company or company_id in skipped_not_due:
            continue

        if last_searched_at is not None and last_searched_at > due_before:
            skipped_not_due.add(company_id)
            continue

        by_company[company_id] = CompanyQuery(
            company_id=company_id, name=name, watermark=last_searched_at
        )

    return CompanyQueryBatch(
        theme_ids=[int(theme_id) for theme_id in theme_ids],
        queries=list(by_company.values()),
        skipped_no_company=len(skipped_names),
        skipped_not_due=len(skipped_not_due),
    )


def fetch_due_company_queries(
    theme_ids: list[int], interval_hours: int, now: datetime
) -> CompanyQueryBatch:
    """테마 편입 기업 중 재검색 시점이 된 기업 조회"""

    unique_ids = sorted({int(theme_id) for theme_id in theme_ids})
    if not unique_ids:
        return group_company_rows([], [], interval_hours, now)

    with session_scope() as session:
        rows = session.execute(SELECT_THEME_COMPANIES_SQL, {"theme_ids": unique_ids}).fetchall()

    return group_company_rows(unique_ids, [tuple(row) for row in rows], interval_hours, now)


def mark_companies_searched(company_ids: list[int], searched_at: datetime) -> int:
    """검색을 마친 기업의 search_history.last_searched_at 을 upsert 한다. 갱신 행 수를 반환한다.

    런 시작 시각으로 마킹한다 — 이 값이 다음 런의 워터마크라, 런 종료 시각으로 마킹하면 런
    도중 발행 기사가 창 밖으로 빠진다. 검색에 실패한 기업은 호출자가 빼고 넘긴다 — 그 기업은
    이전 워터마크를 유지해 다음 런에 같은 창을 다시 읽는다.
    """

    unique_ids = sorted({int(company_id) for company_id in company_ids if company_id})

    if not unique_ids:
        return 0

    with session_scope() as session:
        result = session.execute(
            UPSERT_SEARCH_HISTORY_SQL, {"ids": unique_ids, "searched_at": searched_at}
        )

        return result.rowcount or 0
