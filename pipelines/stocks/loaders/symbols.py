from __future__ import annotations

import json

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from pipelines.stocks.models import StockSymbol, SymbolSyncResult

# upsert SQL
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
    ON CONFLICT (symbol) DO UPDATE SET
      standard_code = EXCLUDED.standard_code,
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
          AND symbol NOT IN :active_symbols
        """
    )
    .bindparams(bindparam("markets", expanding=True))
    .bindparams(bindparam("active_symbols", expanding=True))
)


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

    payload = [_to_payload(symbol) for symbol in symbols]
    session.execute(UPSERT_SYMBOL_SQL, payload)

    markets = sorted({symbol.market for symbol in symbols})
    active_symbols = sorted({symbol.symbol for symbol in symbols})
    result = session.execute(
        DEACTIVATE_MISSING_SYMBOLS_SQL,
        {"markets": markets, "active_symbols": active_symbols},
    )

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
