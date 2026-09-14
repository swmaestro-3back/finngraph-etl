"""검색 대상 기업 그룹핑·재검색 판정 단위 테스트 (DB 불필요)."""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

NOW = datetime(2026, 9, 10, 12, tzinfo=SEOUL_TIMEZONE)
OLD = NOW - timedelta(hours=3)  # 2시간 간격을 넘김
RECENT = NOW - timedelta(minutes=30)  # 아직 간격 안


def test_group_dedupes_company_and_keeps_first_stock_name(caplog):
    from pipelines.news.repositories.search_history import CompanyQuery, group_company_rows

    rows = [
        (100, "엘앤에프", OLD),
        (100, "엘앤에프", OLD),  # 같은 기업이 두 테마에 편입 — 한 번만 검색
        (200, "현대차", None),
        (200, "현대차2우B", None),  # 같은 기업의 다른 종목 — 첫 종목명으로 검색
        (None, "미이관", None),  # company_id 없음 → 제외
    ]

    batch = group_company_rows(theme_ids=[10, 11], rows=rows, interval_hours=2, now=NOW)

    assert batch.theme_ids == [10, 11]
    assert batch.queries == [
        CompanyQuery(company_id=100, name="엘앤에프", watermark=OLD),
        CompanyQuery(company_id=200, name="현대차", watermark=None),
    ]
    assert batch.skipped_no_company == 1
    assert batch.skipped_not_due == 0
    assert "미이관" in caplog.text


def test_group_skips_company_searched_within_interval():
    from pipelines.news.repositories.search_history import CompanyQuery, group_company_rows

    rows = [(100, "엘앤에프", RECENT), (200, "에코프로", OLD)]

    batch = group_company_rows(theme_ids=[10], rows=rows, interval_hours=2, now=NOW)

    assert batch.queries == [CompanyQuery(company_id=200, name="에코프로", watermark=OLD)]
    assert batch.skipped_not_due == 1


def test_group_due_predicate_is_plain_interval():
    from pipelines.news.repositories.search_history import group_company_rows

    exactly = NOW - timedelta(hours=2)
    just_under = NOW - timedelta(hours=1, minutes=55)
    rows = [(100, "a", exactly), (200, "b", just_under)]

    batch = group_company_rows(theme_ids=[10], rows=rows, interval_hours=2, now=NOW)

    assert [q.company_id for q in batch.queries] == [100]
    # 간격 0 시간이면 방금 검색한 기업도 다시 대상이다
    assert len(group_company_rows([10], rows, interval_hours=0, now=NOW).queries) == 2


def test_group_with_no_rows():
    from pipelines.news.repositories.search_history import group_company_rows

    batch = group_company_rows(theme_ids=[10], rows=[], interval_hours=2, now=NOW)

    assert batch.theme_ids == [10]
    assert batch.queries == []
    assert batch.skipped_no_company == 0
    assert batch.skipped_not_due == 0


def test_group_counts_orphan_stock_once(caplog):
    from pipelines.news.repositories.search_history import group_company_rows

    rows = [(None, "orphan", None), (None, "orphan", None)]

    batch = group_company_rows(theme_ids=[10, 11], rows=rows, interval_hours=2, now=NOW)

    assert batch.queries == []
    assert batch.skipped_no_company == 1
    assert caplog.text.count("orphan") == 1
