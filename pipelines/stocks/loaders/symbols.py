from __future__ import annotations

import json

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from pipelines.stocks.models import StockSymbol, SymbolSyncResult

# upsert SQL
#
# conflict target이 symbol이 아니라 standard_code인 이유:
#   symbol 부분 유니크(WHERE is_active)를 쓰면 종목이 하루 master에서 빠졌다가 다시
#   나타날 때 기존 비활성 행과 충돌하지 않아 중복 행이 생긴다. 표준코드는 활성 여부와
#   무관하게 같은 종목을 가리키므로 재등장 시 같은 행을 되살린다. 반대로 종목코드가
#   재사용되어 다른 회사가 새 표준코드로 들어오면 새 행이 되고 옛 행은 비활성으로 남는다.
#   자세한 근거는 migrations/versions/20260804_02_stocks_surrogate_id.sql 참고.
UPSERT_SYMBOL_SQL = text(
    """
    INSERT INTO stocks (
      symbol,
      standard_code,
      name,
      market,
      listed_date,
      is_active,
      trading_suspended,
      under_administration,
      delisting_trade,
      preferred_stock,
      etp,
      spac,
      listed_shares,
      par_value,
      capital,
      source,
      synced_at,
      inactive_at,
      raw_attributes,
      created_at,
      updated_at
    )
    VALUES (
      :symbol,
      :standard_code,
      :name,
      :market,
      :listed_date,
      :is_active,
      :trading_suspended,
      :under_administration,
      :delisting_trade,
      :preferred_stock,
      :etp,
      :spac,
      :listed_shares,
      :par_value,
      :capital,
      :source,
      :synced_at,
      :inactive_at,
      CAST(:raw_attributes AS jsonb),
      now(),
      now()
    )
    ON CONFLICT (standard_code) DO UPDATE SET
      symbol = EXCLUDED.symbol,
      name = EXCLUDED.name,
      market = EXCLUDED.market,
      listed_date = EXCLUDED.listed_date,
      is_active = EXCLUDED.is_active,
      trading_suspended = EXCLUDED.trading_suspended,
      under_administration = EXCLUDED.under_administration,
      delisting_trade = EXCLUDED.delisting_trade,
      preferred_stock = EXCLUDED.preferred_stock,
      etp = EXCLUDED.etp,
      spac = EXCLUDED.spac,
      listed_shares = EXCLUDED.listed_shares,
      par_value = EXCLUDED.par_value,
      capital = EXCLUDED.capital,
      source = EXCLUDED.source,
      synced_at = EXCLUDED.synced_at,
      inactive_at = EXCLUDED.inactive_at,
      raw_attributes = EXCLUDED.raw_attributes,
      updated_at = now()
    """
)

# 사라진 기존 종목을 inactive 하기위한 SQL문
#
# 판정 기준이 symbol이 아니라 standard_code다. 종목코드가 재사용되면 같은 symbol을 옛 회사와
# 새 회사가 공유하게 되는데, symbol 기준으로는 "오늘 master에 있음"으로 잡혀 옛 행이 활성인
# 채로 남는다. 그러면 활성 종목 부분 유니크(stocks_active_symbol_uk)에 두 행이 걸린다.
DEACTIVATE_MISSING_SYMBOLS_SQL = (
    text(
        """
        UPDATE stocks
        SET
          is_active = false,
          inactive_at = COALESCE(inactive_at, now()),
          updated_at = now()
        WHERE market IN :markets
          AND is_active = true
          AND standard_code NOT IN :active_standard_codes
        """
    )
    .bindparams(bindparam("markets", expanding=True))
    .bindparams(bindparam("active_standard_codes", expanding=True))
)


# 서비스 제공 대상 종목 목록
#
# 우선주·ETP·SPAC를 뺀다. 수집 범위와 제공 범위를 구분하는 원칙(master는 전량 적재)은
# 파일 하나로 끝나는 master에나 적용된다. 재무·수급·배당·분봉은 종목당 API 1회씩이라
# 전량을 돌면 호출 수가 4,400건이 되고, 그중 1,700건은 화면에 나가지도 않는다.
SELECT_SERVICEABLE_SYMBOLS_SQL = text(
    """
    SELECT s.id, s.symbol
      FROM stocks AS s
     WHERE s.is_active
       AND NOT s.preferred_stock
       AND NOT s.etp
       AND NOT s.spac
     ORDER BY s.symbol
    """
)

SELECT_ACTIVE_STOCK_IDS_SQL = text("SELECT symbol, id FROM stocks WHERE is_active")


def fetch_serviceable_stocks(session: Session, limit: int | None = None) -> list[tuple[int, str]]:
    """시세·재무 수집 대상 종목을 (stock_id, symbol)로 반환한다.

    Args:
        session (Session): DB 세션.
        limit (int | None): 상한. 호출 한도 때문에 배치를 쪼갤 때 쓴다.

    Returns:
        list[tuple[int, str]]: (stock_id, symbol) 목록. 단축코드 오름차순.
    """

    rows = session.execute(SELECT_SERVICEABLE_SYMBOLS_SQL).all()
    result = [(row.id, row.symbol) for row in rows]
    return result[:limit] if limit else result


def fetch_active_stock_ids(session: Session) -> dict[str, int]:
    """활성 종목의 단축코드 → stock_id 매핑.

    extractor가 돌려주는 단축코드를 시세 테이블의 키로 바꾸는 데 쓴다. 활성 종목의
    단축코드는 부분 유니크(stocks_active_symbol_uk)라 중복이 없다.
    """

    return {row.symbol: row.id for row in session.execute(SELECT_ACTIVE_STOCK_IDS_SQL)}


def sync_symbols(session: Session, symbols: list[StockSymbol]) -> SymbolSyncResult:
    """Symbol 정보를 DB에 저장

    Args:
        session(Session): DB Session
        symbols(list[StockSymbol]): 불러온 symbol list

    Returns:
        SymbolSyncResult:
            upserted_count: upsert row count \n
            inactive_count: inactive row count
    """
    if not symbols:
        return SymbolSyncResult(upserted_count=0, inactive_count=0)

    # 비활성 처리를 upsert보다 먼저 한다. 종목코드가 재사용된 경우 옛 행이 활성인 채로
    # 남아 있으면 같은 symbol을 쓰는 새 행 insert가 활성 종목 부분 유니크에 걸린다.
    markets = sorted({symbol.market for symbol in symbols})
    active_standard_codes = sorted({symbol.standard_code for symbol in symbols})
    result = session.execute(
        DEACTIVATE_MISSING_SYMBOLS_SQL,
        {"markets": markets, "active_standard_codes": active_standard_codes},
    )

    payload = [_to_payload(symbol) for symbol in symbols]
    session.execute(UPSERT_SYMBOL_SQL, payload)

    return SymbolSyncResult(
        upserted_count=len(payload),
        inactive_count=result.rowcount or 0,
    )


def upsert_symbols(session: Session, symbols: list[StockSymbol]) -> int:
    return sync_symbols(session, symbols).upserted_count


def _to_payload(symbol: StockSymbol) -> dict[str, object]:
    """SQL문에 사용할 dict 자료형으로 변환합니다.

    Args:
        symbol (StockSymbol): 주식 정보

    Returns:
        dict: stock dict
    """
    return {
        "symbol": symbol.symbol,
        "standard_code": symbol.standard_code,
        "name": symbol.name,
        "market": symbol.market,
        "listed_date": symbol.listed_date,
        "is_active": symbol.is_active,
        "trading_suspended": symbol.trading_suspended,
        "under_administration": symbol.under_administration,
        "delisting_trade": symbol.delisting_trade,
        "preferred_stock": symbol.preferred_stock,
        "etp": symbol.etp,
        "spac": symbol.spac,
        "listed_shares": symbol.listed_shares,
        "par_value": symbol.par_value,
        "capital": symbol.capital,
        "source": symbol.source,
        "synced_at": symbol.synced_at,
        "inactive_at": symbol.inactive_at,
        "raw_attributes": (
            json.dumps(symbol.raw_attributes, ensure_ascii=False)
            if symbol.raw_attributes is not None
            else None
        ),
    }
