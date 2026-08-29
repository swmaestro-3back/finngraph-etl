"""companies 로더 통합 테스트 — 법인 생성·상장 토글·종목 연결·별칭.

실제 Postgres에 붙어 두 로더가 맞물려 도는지 검증한다. `integration` 마커가 붙어 CI
unit-test job에서는 제외되고 db-integration job에서만 실행된다.
로컬은 `docker compose up -d db` 후 `pytest -m integration`.

**역할 분담이 이 테스트의 핵심이다.**

    법인 행을 만드는 것   → link_listed_corp_codes (DART corpCode.xml)
    상장 여부를 정하는 것 → sync_listed_companies (KIS 종목 마스터)

그래서 테스트도 두 단계를 그대로 흉내낸다. corpCode에 없는 종목은 stocks에 아무리 있어도
법인이 되지 않는다 — 예전처럼 stocks에서 종목을 걸러 법인을 만들면 공모펀드·신주인수권처럼
플래그에 안 걸리는 상품이 새어 들어오기 때문이다.

검증 대상은 다섯 가지다.

1. corpCode에 있는 보통주는 법인이 되고 상장으로 켜진다 — 종목명과 단축코드가 모두
   별칭으로 들어간다.
2. corpCode에 없으면 법인이 생기지 않는다 — stocks에만 남고 company_id는 NULL이다.
3. 우선주·ETP·SPAC는 법인이 아니다 — 삼성전자우는 삼성전자와 같은 법인이고 ETF는 애초에
   사업을 하는 법인이 아니다.
4. 활성 목록에서 사라지면 상장폐지로 내려간다 — 법인 행은 남고 is_listed만 꺼진다.
5. 재실행이 멱등이다 — 매일 도는 job이라 두 번째 실행에서 법인·별칭이 늘거나 연결이
   다시 갱신되면 안 된다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.companies.loaders.dart import (
    insert_dart_name_aliases,
    link_listed_corp_codes,
    seed_curated_aliases,
)
from pipelines.companies.loaders.postgres import sync_listed_companies
from pipelines.companies.models import CompanySyncResult, DartCorp
from pipelines.stocks.loaders.tickers import sync_tickers
from pipelines.stocks.models import SECURITY_GROUP_STOCK, StockTicker

pytestmark = pytest.mark.integration

# 운영 데이터와 섞이지 않도록 테스트 전용 시장 구분값.
TEST_MARKET = "PYTEST_COMPANIES"

COMMON_SYMBOL = "999901"
PREFERRED_SYMBOL = "999902"
ETP_SYMBOL = "999903"
UNKNOWN_SYMBOL = "999904"

TEST_SYMBOLS = (COMMON_SYMBOL, PREFERRED_SYMBOL, ETP_SYMBOL, UNKNOWN_SYMBOL)

# corp_code는 8자리다. 실재하는 법인과 겹치지 않도록 99로 시작하는 값을 쓴다.
CORP_CODES = {ticker: f"99{ticker}" for ticker in TEST_SYMBOLS}


def _stock(
    ticker: str,
    name: str,
    security_group: str = SECURITY_GROUP_STOCK,
    **flags: bool,
) -> StockTicker:
    return StockTicker(
        ticker=ticker,
        standard_code=f"KR7{ticker}001",
        name=name,
        market=TEST_MARKET,
        security_group=security_group,
        **flags,
    )


def _load_stocks(*tickers: StockTicker) -> None:
    with session_scope() as session:
        sync_tickers(session, list(tickers))


def _load_corp_codes(*tickers: str) -> None:
    """DART corpCode.xml 동기화를 흉내내 법인 행을 만든다."""

    corps = [
        DartCorp(corp_code=CORP_CODES[ticker], name=f"법인{ticker}", stock_code=ticker)
        for ticker in tickers
    ]
    with session_scope() as session:
        link_listed_corp_codes(session, corps)


def _sync() -> CompanySyncResult:
    with session_scope() as session:
        return sync_listed_companies(session)


def _companies() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT ticker, name, country, is_listed, delisted_at
                  FROM companies
                 WHERE corp_code = ANY(:corp_codes)
                 ORDER BY ticker
                """
            ),
            {"corp_codes": list(CORP_CODES.values())},
        )
        return [dict(row._mapping) for row in rows]


def _stock_links() -> dict[str, int | None]:
    with session_scope() as session:
        rows = session.execute(
            text("SELECT ticker, company_id FROM stocks WHERE market = :market"),
            {"market": TEST_MARKET},
        )
        return {row.ticker: row.company_id for row in rows}


def _aliases() -> set[str]:
    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT a.alias
                  FROM company_aliases AS a
                  JOIN companies AS c ON c.id = a.company_id
                 WHERE c.corp_code = ANY(:corp_codes)
                """
            ),
            {"corp_codes": list(CORP_CODES.values())},
        )
        return {row.alias for row in rows}


@pytest.fixture(autouse=True)
def _cleanup():
    def purge() -> None:
        with session_scope() as session:
            # stocks가 companies를 참조하므로 종목을 먼저 지운다.
            # company_aliases는 ON DELETE CASCADE라 companies와 함께 사라진다.
            session.execute(
                text("DELETE FROM stocks WHERE market = :market"), {"market": TEST_MARKET}
            )
            # market이 아니라 corp_code로 지운다. 상장으로 켜지지 않은 법인은 market이
            # NULL로 남아 시장 조건에 걸리지 않는다.
            session.execute(
                text("DELETE FROM companies WHERE corp_code = ANY(:corp_codes)"),
                {"corp_codes": list(CORP_CODES.values())},
            )

    purge()
    yield
    purge()


def test_listed_common_stock_becomes_company_with_aliases() -> None:
    """corpCode에 있는 보통주는 법인이 되고 상장으로 켜진다."""
    # listed_count는 전 테이블 기준 카운터다. 다른 테스트가 남긴 종목까지 세지 않도록
    # 먼저 한 번 돌려 상태를 정리한 뒤, 이 테스트가 만든 변화만 세게 한다.
    _sync()

    _load_stocks(_stock(COMMON_SYMBOL, "테스트전자"))
    _load_corp_codes(COMMON_SYMBOL)

    result = _sync()

    assert result.listed_count == 1
    assert result.delisted_count == 0

    companies = _companies()
    assert len(companies) == 1
    assert companies[0]["ticker"] == COMMON_SYMBOL
    assert companies[0]["country"] == "KR"
    assert companies[0]["is_listed"] is True
    assert companies[0]["delisted_at"] is None

    # 이름은 KIS 종목명으로 덮인다. DART 법인명("법인999901")은 표기가 달라 서비스에 쓰지 않는다.
    assert companies[0]["name"] == "테스트전자"

    assert _stock_links()[COMMON_SYMBOL] is not None
    assert _aliases() == {"테스트전자", COMMON_SYMBOL}


def test_stock_without_corp_code_creates_no_company() -> None:
    """corpCode에 없으면 법인을 만들지 않는다.

    이 성질이 예전 오염(공모펀드 F701…, 신주인수권 J…WR)을 구조적으로 막는다.
    stocks 쪽에서 상품 종류를 열거해 뺄 필요가 없다.
    """
    _load_stocks(_stock(UNKNOWN_SYMBOL, "코드없는종목"))

    result = _sync()

    assert result.listed_count == 0
    assert _companies() == []
    assert _stock_links()[UNKNOWN_SYMBOL] is None
    assert _aliases() == set()


def test_corp_code_absence_keeps_a_stock_out_of_companies() -> None:
    """corpCode에 없는 종목은 법인이 되지 않는다.

    우선주·ETF는 수집 경계(kis_stock_master.is_collectible)에서 걸러지므로 stocks에
    애초에 들어오지 않는다. 여기서는 corpCode 쪽 조건만 본다 —
    종류 판정 테스트는 tests/stocks/test_kis_stock_master.py 에 있다.
    """

    _load_stocks(
        _stock(COMMON_SYMBOL, "테스트전자"),
        _stock(UNKNOWN_SYMBOL, "테스트미등록"),
    )
    _load_corp_codes(COMMON_SYMBOL)

    _sync()

    assert [row["ticker"] for row in _companies()] == [COMMON_SYMBOL], (
        "corpCode에 있는 종목만 법인이 되어야 한다"
    )

    links = _stock_links()
    assert links[COMMON_SYMBOL] is not None
    assert links[UNKNOWN_SYMBOL] is None

    # 법인에 연결되지 않은 종목의 이름은 별칭으로도 들어가지 않는다.
    assert "테스트미등록" not in _aliases()


def test_delisting_turns_the_flag_off_and_keeps_the_company() -> None:
    """활성 목록에서 사라지면 상장만 꺼진다. 법인 행과 재무는 남는다."""
    _load_stocks(_stock(COMMON_SYMBOL, "테스트전자"))
    _load_corp_codes(COMMON_SYMBOL)
    _sync()

    with session_scope() as session:
        session.execute(
            text("UPDATE stocks SET is_active = false WHERE ticker = :ticker"),
            {"ticker": COMMON_SYMBOL},
        )

    result = _sync()

    assert result.delisted_count == 1

    companies = _companies()
    assert len(companies) == 1, "법인 행은 지우지 않는다"
    assert companies[0]["is_listed"] is False
    # delisted_at을 채워야 활성 ticker 부분 유니크에서 빠진다. 안 그러면 단축코드가
    # 재사용될 때 새 법인을 넣을 수 없다.
    assert companies[0]["delisted_at"] is not None


def test_dart_corp_name_saved_as_alias_for_listed_only() -> None:
    """상장 법인의 DART 법인명은 source='DART' 별칭으로 보존된다.

    companies.name은 매일 KIS 종목명으로 덮이므로(테스트전자), DART 법인명(법인999901)은
    별칭이 유일한 보존처다 — 공시 계약상대 역매칭이 이 별칭을 쓴다.
    비상장 법인에는 별칭을 자동으로 붙이지 않는다(동명 법인이 많아 모호해진다).
    """
    _load_stocks(_stock(COMMON_SYMBOL, "테스트전자"))
    _load_corp_codes(COMMON_SYMBOL)
    _sync()

    corps = [
        DartCorp(corp_code=CORP_CODES[COMMON_SYMBOL], name="법인999901", stock_code=COMMON_SYMBOL),
        # 비상장(stock_code 없음) — 별칭이 생기면 안 된다.
        DartCorp(corp_code=CORP_CODES[UNKNOWN_SYMBOL], name="비상장법인", stock_code=None),
        # 마스터에 법인 행이 없는 corp_code — 조용히 건너뛴다.
        DartCorp(corp_code="99999999", name="유령법인", stock_code="999999"),
    ]
    with session_scope() as session:
        added = insert_dart_name_aliases(session, corps)

    assert added == 1
    assert "법인999901" in _aliases()
    assert "비상장법인" not in _aliases()
    assert "유령법인" not in _aliases()

    with session_scope() as session:
        rows = session.execute(
            text(
                """
                SELECT a.source
                  FROM company_aliases AS a
                  JOIN companies AS c ON c.id = a.company_id
                 WHERE a.alias = '법인999901' AND c.corp_code = :corp_code
                """
            ),
            {"corp_code": CORP_CODES[COMMON_SYMBOL]},
        ).all()
    assert [row.source for row in rows] == ["DART"]

    # 매주 도는 job이므로 재실행은 멱등이어야 한다.
    with session_scope() as session:
        assert insert_dart_name_aliases(session, corps) == 0


def test_curated_aliases_seed_resolves_master_name_uniquely() -> None:
    """수동 관리 별칭(사명변경·통용표기)은 마스터명이 유일하게 해석될 때만 들어간다.

    시드는 (별칭, 마스터명) 쌍이라 company_id를 이름으로 찾아야 하는데, 같은 이름의
    법인이 둘이면 어느 쪽인지 알 수 없다 — 그때는 넣지 않는 게 맞다.
    """
    _load_stocks(_stock(COMMON_SYMBOL, "테스트전자"))
    _load_corp_codes(COMMON_SYMBOL)
    _sync()

    seed = [
        ("구테스트전자", "테스트전자"),  # 마스터명 유일 → 들어간다
        ("유령별칭", "존재하지않는법인"),  # 마스터에 없음 → 건너뛴다
    ]
    with session_scope() as session:
        added = seed_curated_aliases(session, seed)

    assert added == 1
    assert "구테스트전자" in _aliases()
    assert "유령별칭" not in _aliases()

    with session_scope() as session:
        source = session.execute(
            text("SELECT source FROM company_aliases WHERE alias = '구테스트전자'")
        ).scalar_one()
    assert source == "CURATED"

    with session_scope() as session:
        assert seed_curated_aliases(session, seed) == 0, "재실행은 멱등이어야 한다"


def test_rerun_is_idempotent() -> None:
    """매일 도는 job이므로 두 번째 실행에서 아무것도 바뀌지 않아야 한다."""
    _load_stocks(_stock(COMMON_SYMBOL, "테스트전자"))
    _load_corp_codes(COMMON_SYMBOL)
    _sync()

    result = _sync()

    assert result.listed_count == 0, "이미 상장으로 켜져 있고 이름·시장도 같다"
    assert result.delisted_count == 0
    assert result.linked_count == 0, "이미 같은 법인을 가리키므로 다시 연결하지 않는다"
    assert result.alias_count == 0, "이미 있는 별칭은 추가되지 않는다"

    assert len(_companies()) == 1
    assert _aliases() == {"테스트전자", COMMON_SYMBOL}
