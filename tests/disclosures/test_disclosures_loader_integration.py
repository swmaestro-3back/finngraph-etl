"""disclosures 로더 통합 테스트 — upsert 멱등성과 JSONB 왕복.

실제 Postgres에 붙는다. `integration` 마커가 붙어 CI unit-test job에서는 제외된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

검증 대상은 네 가지다.

1. 신규 접수번호는 행이 생기고 fields/meta 가 JSONB 로 온전히 돌아온다.
2. 같은 접수번호 재적재는 행을 늘리지 않고 내용만 덮어쓴다 — 일일 lookback 겹침과
   재파싱이 이 성질에 기댄다.
3. fetch_existing_rcept_nos 가 적재된 것만 돌려준다 — 원문 재조회 스킵의 근거다.
4. fetch_supply_edges 는 제출사·계약상대 양쪽 ticker 가 있고 companies 에 정규명이
   해석되는 공시만 돌려준다 — 근거 원장 대상 선정의 근거다.
5. upsert_relation_sources 는 근거 원장에 disclosure 행을 멱등 적재하고, 정정 시
   내용을 덮어쓴다.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.disclosures.loaders.postgres import (
    delete_stale_relation_sources,
    fetch_disclosure_edge_keys,
    fetch_existing_rcept_nos,
    fetch_supply_edges,
    upsert_disclosures,
    upsert_relation_sources,
)
from pipelines.disclosures.models import DisclosureRecord

pytestmark = pytest.mark.integration

# 운영 데이터와 섞이지 않도록 테스트 전용 접수번호·ticker 대역.
TEST_RCEPT_NOS = ("99999901000001", "99999901000002")
TEST_TICKERS = {"999901": "테스트공급사", "999902": "테스트수요사"}


@pytest.fixture(autouse=True)
def cleanup():
    # fetch_supply_edges 의 companies 조인을 위해 ticker 정규명 시드를 깐다.
    with session_scope() as session:
        for ticker, name in TEST_TICKERS.items():
            session.execute(
                text(
                    """
                    INSERT INTO companies (name, ticker, is_listed, country)
                    VALUES (:name, :ticker, TRUE, 'KR')
                    """
                ),
                {"name": name, "ticker": ticker},
            )
    yield
    with session_scope() as session:
        session.execute(
            text("DELETE FROM relation_sources WHERE rcept_no = ANY(:rcept_nos)"),
            {"rcept_nos": list(TEST_RCEPT_NOS)},
        )
        session.execute(
            text("DELETE FROM disclosures WHERE rcept_no = ANY(:rcept_nos)"),
            {"rcept_nos": list(TEST_RCEPT_NOS)},
        )
        session.execute(
            text("DELETE FROM companies WHERE ticker = ANY(:tickers)"),
            {"tickers": list(TEST_TICKERS)},
        )


def make_record(
    rcept_no: str,
    contract_amount: int,
    ticker: str | None = None,
    counterparty_ticker: str | None = None,
) -> DisclosureRecord:
    return DisclosureRecord(
        rcept_no=rcept_no,
        corp_code="99999999",
        company_id=None,
        ticker=ticker,
        corp_cls="KOSDAQ",
        report_nm="단일판매ㆍ공급계약체결",
        is_correction=False,
        rcept_dt=date(2026, 8, 21),
        flr_nm="파이테스트",
        link=f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
        contract_type="기타 판매ㆍ공급계약",
        contract_name="테스트 계약",
        counterparty="테스트상대",
        counterparty_corp_name=None,
        counterparty_corp_code=None,
        counterparty_ticker=counterparty_ticker,
        start_date=date(2026, 9, 1),
        end_date=None,
        order_date=None,
        fields={"contract_amount": contract_amount},
        meta={"fetched_at": "2026-08-21T00:00:00+09:00", "parser_version": 2},
    )


def test_upsert_roundtrips_jsonb_and_is_idempotent() -> None:
    record = make_record(TEST_RCEPT_NOS[0], contract_amount=100)

    with session_scope() as session:
        assert upsert_disclosures(session, [record]) == 1

    updated = make_record(TEST_RCEPT_NOS[0], contract_amount=200)
    with session_scope() as session:
        upsert_disclosures(session, [updated])

    with session_scope() as session:
        rows = session.execute(
            text(
                "SELECT rcept_no, counterparty, start_date, fields, meta"
                "  FROM disclosures WHERE rcept_no = :rcept_no"
            ),
            {"rcept_no": TEST_RCEPT_NOS[0]},
        ).all()

    assert len(rows) == 1  # 재적재해도 행이 늘지 않는다
    assert rows[0].fields["contract_amount"] == 200
    assert rows[0].counterparty == "테스트상대"  # 승격 컬럼
    assert rows[0].start_date == date(2026, 9, 1)
    assert rows[0].meta["parser_version"] == 2


def test_fetch_existing_rcept_nos_returns_only_loaded() -> None:
    with session_scope() as session:
        upsert_disclosures(session, [make_record(TEST_RCEPT_NOS[0], 100)])

    with session_scope() as session:
        existing = fetch_existing_rcept_nos(session, list(TEST_RCEPT_NOS))

    assert existing == {TEST_RCEPT_NOS[0]}
    with session_scope() as session:
        assert fetch_existing_rcept_nos(session, []) == set()


def test_fetch_supply_edges_requires_both_tickers() -> None:
    with session_scope() as session:
        upsert_disclosures(
            session,
            [
                # 양쪽 ticker 있음 -> 간선 대상
                make_record(TEST_RCEPT_NOS[0], 100, ticker="999901", counterparty_ticker="999902"),
                # 계약상대 ticker 없음 -> 제외
                make_record(TEST_RCEPT_NOS[1], 100, ticker="999901"),
            ],
        )

    with session_scope() as session:
        edges = {e.rcept_no: e for e in fetch_supply_edges(session)}

    assert TEST_RCEPT_NOS[0] in edges
    assert TEST_RCEPT_NOS[1] not in edges
    edge = edges[TEST_RCEPT_NOS[0]]
    assert edge.filer_ticker == "999901"
    assert edge.filer_name == TEST_TICKERS["999901"]
    assert edge.counterparty_ticker == "999902"
    assert edge.counterparty_name == TEST_TICKERS["999902"]
    assert edge.item == "테스트 계약"  # contract_name 우선
    assert edge.rcept_dt == date(2026, 8, 21)


def test_upsert_relation_sources_is_idempotent_and_overwrites() -> None:
    with session_scope() as session:
        upsert_disclosures(
            session,
            [make_record(TEST_RCEPT_NOS[0], 100, ticker="999901", counterparty_ticker="999902")],
        )
        edges = fetch_supply_edges(session)
        edges = [e for e in edges if e.rcept_no == TEST_RCEPT_NOS[0]]
        assert upsert_relation_sources(session, edges) == 1
        # 재적재는 행을 늘리지 않는다
        upsert_relation_sources(session, edges)

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT source_type, subject_name, relation, object_name,
                       subject_code, object_code, mentioned_at, item,
                       polarity, tense
                  FROM relation_sources
                 WHERE rcept_no = :rcept_no
                """
            ),
            {"rcept_no": TEST_RCEPT_NOS[0]},
        ).all()

    assert len(rows) == 1
    row = rows[0]
    assert row.source_type == "disclosure"
    assert row.subject_name == TEST_TICKERS["999901"]
    assert row.relation == "SUPPLIES_TO"
    assert row.object_name == TEST_TICKERS["999902"]
    assert row.subject_code == "999901"
    assert row.object_code == "999902"
    assert row.mentioned_at == date(2026, 8, 21)
    assert row.item == "테스트 계약"
    # 공시는 확정 사실 — 상수로 들어간다
    assert row.polarity == "affirmed"
    assert row.tense == "past_or_present_fact"


def test_delete_stale_relation_sources_reconciles_ledger() -> None:
    with session_scope() as session:
        upsert_disclosures(
            session,
            [
                make_record(TEST_RCEPT_NOS[0], 100, ticker="999901", counterparty_ticker="999902"),
                make_record(TEST_RCEPT_NOS[1], 200, ticker="999902", counterparty_ticker="999901"),
            ],
        )
        edges = [e for e in fetch_supply_edges(session) if e.rcept_no in TEST_RCEPT_NOS]
        upsert_relation_sources(session, edges)
        key = (TEST_TICKERS["999901"], "SUPPLIES_TO", TEST_TICKERS["999902"])
        assert key in fetch_disclosure_edge_keys(session)

        # 빈 입력은 전량 삭제가 아니라 no-op 이어야 한다(원장 보호 가드).
        assert delete_stale_relation_sources(session, []) == 0

        # 이번 원천에서 첫 공시가 빠졌다(정정/상폐 시나리오) → 그 행만 회수되고
        # 남아 있는 공시(운영 데이터 대역)는 건드리지 않는다.
        kept = [e for e in edges if e.rcept_no == TEST_RCEPT_NOS[1]]
        delete_stale_relation_sources(session, kept)
        remaining = session.execute(
            text("SELECT count(*) FROM relation_sources WHERE rcept_no = :rcept_no"),
            {"rcept_no": TEST_RCEPT_NOS[0]},
        ).scalar_one()
        kept_count = session.execute(
            text("SELECT count(*) FROM relation_sources WHERE rcept_no = :rcept_no"),
            {"rcept_no": TEST_RCEPT_NOS[1]},
        ).scalar_one()

    assert remaining == 0
    assert kept_count == 1
