"""개체 사전 변환 규칙 단위 테스트 — 충돌·우선순위·길이·중복."""

from __future__ import annotations

from pipelines.companies.models import GazetteerAlias
from pipelines.companies.transformers.gazetteer import build_entries


def _alias(alias: str, company_id: int, source: str = "DART") -> GazetteerAlias:
    return GazetteerAlias(
        alias=alias,
        company_id=company_id,
        stock_id=company_id * 10,
        ticker=f"T{company_id}",
        canonical_name=f"기업{company_id}",
        source=source,
    )


def _pairs(entries: list[GazetteerAlias]) -> list[tuple[str, int, str]]:
    return [(entry.alias, entry.company_id, entry.source) for entry in entries]


def test_unique_aliases_pass_through_sorted():
    entries, collisions = build_entries([_alias("삼성전자", 1, "NAME"), _alias("기아", 2, "NAME")])

    assert _pairs(entries) == [("기아", 2, "NAME"), ("삼성전자", 1, "NAME")]
    assert collisions == {}


def test_same_company_duplicates_keep_highest_priority_source():
    entries, _ = build_entries(
        [_alias("삼성전자", 1, "STOCK_NAME"), _alias("삼성전자", 1, "NAME"), _alias("삼성전자", 1)]
    )

    assert _pairs(entries) == [("삼성전자", 1, "NAME")]


def test_alias_shared_by_equal_priority_companies_is_dropped():
    entries, collisions = build_entries([_alias("대덕", 1), _alias("대덕", 2)])

    assert entries == []
    assert collisions == {"대덕": ("기업1", "기업2")}


def test_canonical_name_beats_other_companys_secondary_alias():
    entries, collisions = build_entries([_alias("카프로", 1, "NAME"), _alias("카프로", 2, "DART")])

    assert _pairs(entries) == [("카프로", 1, "NAME")]
    assert collisions == {}


def test_curated_beats_everything():
    # '구글'이 GOOGL·GOOG 두 회사의 자동 별칭이어도 CURATED 로 지정한 쪽이 가진다
    entries, _ = build_entries(
        [_alias("구글", 1, "STOCK_NAME"), _alias("구글", 2, "NAME"), _alias("구글", 1, "CURATED")]
    )

    assert _pairs(entries) == [("구글", 1, "CURATED")]


def test_short_and_blank_aliases_are_dropped_and_others_stripped():
    entries, _ = build_entries([_alias("볼", 1, "NAME"), _alias("  ", 2), _alias(" 기아 ", 3)])

    assert _pairs(entries) == [("기아", 3, "DART")]
