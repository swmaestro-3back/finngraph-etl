from __future__ import annotations

import json

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from pipelines.stocks.models import StockTicker, TickerSyncResult

# upsert SQL
#
# conflict target이 ticker이 아니라 standard_code인 이유:
#   ticker 부분 유니크(WHERE is_active)를 쓰면 종목이 하루 master에서 빠졌다가 다시
#   나타날 때 기존 비활성 행과 충돌하지 않아 중복 행이 생긴다. 표준코드는 활성 여부와
#   무관하게 같은 종목을 가리키므로 재등장 시 같은 행을 되살린다. 반대로 종목코드가
#   재사용되어 다른 회사가 새 표준코드로 들어오면 새 행이 되고 옛 행은 비활성으로 남는다.
#   자세한 근거는 migrations/versions/20260804_02_stocks_surrogate_id.sql 참고.
UPSERT_TICKER_SQL = text(
    """
    INSERT INTO stocks (
      ticker,
      standard_code,
      name,
      market,
      security_group,
      listed_date,
      is_active,
      trading_suspended,
      under_administration,
      delisting_trade,
      preferred_stock,
      etp,
      spac,
      krx100,
      krx300,
      kosdaq150,
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
      :ticker,
      :standard_code,
      :name,
      :market,
      :security_group,
      :listed_date,
      :is_active,
      :trading_suspended,
      :under_administration,
      :delisting_trade,
      :preferred_stock,
      :etp,
      :spac,
      :krx100,
      :krx300,
      :kosdaq150,
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
      ticker = EXCLUDED.ticker,
      name = EXCLUDED.name,
      market = EXCLUDED.market,
      security_group = EXCLUDED.security_group,
      listed_date = EXCLUDED.listed_date,
      is_active = EXCLUDED.is_active,
      trading_suspended = EXCLUDED.trading_suspended,
      under_administration = EXCLUDED.under_administration,
      delisting_trade = EXCLUDED.delisting_trade,
      preferred_stock = EXCLUDED.preferred_stock,
      etp = EXCLUDED.etp,
      spac = EXCLUDED.spac,
      krx100 = EXCLUDED.krx100,
      krx300 = EXCLUDED.krx300,
      kosdaq150 = EXCLUDED.kosdaq150,
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
# 판정 기준이 ticker이 아니라 standard_code다. 종목코드가 재사용되면 같은 ticker을 옛 회사와
# 새 회사가 공유하게 되는데, ticker 기준으로는 "오늘 master에 있음"으로 잡혀 옛 행이 활성인
# 채로 남는다. 그러면 활성 종목 부분 유니크(stocks_active_ticker_uk)에 두 행이 걸린다.
DEACTIVATE_MISSING_TICKERS_SQL = (
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
# 종류는 여기서 판정하지 않는다. 수집 경계(kis_stock_master.is_collectible)가
# 보통주만 들이므로 stocks 에 있는 것은 이미 다룰 종목이다.
#
# 정렬이 단축코드 순으로 고정이라 상한을 두면 뒤쪽 종목에 순서가 영영 오지 않는다.
SELECT_SERVICEABLE_TICKERS_SQL = text(
    """
    SELECT s.id, s.ticker
      FROM stocks AS s
     WHERE EXISTS (SELECT 1 FROM service_companies AS u WHERE u.company_id = s.company_id)
       AND s.is_active
     ORDER BY s.ticker
    """
)

# 단축코드로 직접 지정한 종목
#
# service_companies 조건을 걸지 않는다. 수동 백필은 "이 종목이 이상하니 다시 받아봐라"
# 라는 지시라, 서비스 대상 여부와 무관하게 지정한 것을 받아야 한다. 대신 찾지 못한
# 코드는 호출부가 알 수 있게 돌려준다.
SELECT_STOCKS_BY_TICKERS_SQL = (
    text(
        """
        SELECT s.id, s.ticker
          FROM stocks AS s
         WHERE s.is_active
           AND s.ticker IN :tickers
         ORDER BY s.ticker
        """
    )
).bindparams(bindparam("tickers", expanding=True))

SELECT_ACTIVE_STOCK_IDS_SQL = text("SELECT ticker, id FROM stocks WHERE is_active")


def fetch_serviceable_stocks(session: Session, limit: int | None = None) -> list[tuple[int, str]]:
    """시세·재무 수집 대상 종목을 (stock_id, ticker)로 반환한다.

    Args:
        session (Session): DB 세션.
        limit (int | None): 상한. 수동 점검용이고 배치에서는 생략한다.

    Returns:
        list[tuple[int, str]]: (stock_id, ticker) 목록. 단축코드 오름차순.
    """

    rows = session.execute(SELECT_SERVICEABLE_TICKERS_SQL).all()
    result = [(row.id, row.ticker) for row in rows]
    return result[:limit] if limit else result


def fetch_stocks_by_tickers(
    session: Session, tickers: list[str]
) -> tuple[list[tuple[int, str]], list[str]]:
    """단축코드로 지정한 활성 종목을 찾는다.

    Args:
        session (Session): DB 세션.
        tickers (list[str]): 단축코드 목록.

    Returns:
        tuple[list[tuple[int, str]], list[str]]: (찾은 (stock_id, ticker) 목록,
            찾지 못한 단축코드 목록). 찾지 못한 코드를 조용히 버리지 않는 이유는
            upsert 쪽과 같다 — "수집은 됐는데 화면에 없는" 상태를 추적할 수 없게 된다.
    """

    if not tickers:
        return [], []

    rows = session.execute(SELECT_STOCKS_BY_TICKERS_SQL, {"tickers": tickers}).all()
    found = [(row.id, row.ticker) for row in rows]
    missing = sorted(set(tickers) - {ticker for _, ticker in found})
    return found, missing


def fetch_active_stock_ids(session: Session) -> dict[str, int]:
    """활성 종목의 단축코드 → stock_id 매핑.

    extractor가 돌려주는 단축코드를 시세 테이블의 키로 바꾸는 데 쓴다. 활성 종목의
    단축코드는 부분 유니크(stocks_active_ticker_uk)라 중복이 없다.
    """

    return {row.ticker: row.id for row in session.execute(SELECT_ACTIVE_STOCK_IDS_SQL)}


def sync_tickers(session: Session, tickers: list[StockTicker]) -> TickerSyncResult:
    """Ticker 정보를 DB에 저장

    Args:
        session(Session): DB Session
        tickers(list[StockTicker]): 불러온 ticker list

    Returns:
        TickerSyncResult:
            upserted_count: upsert row count \n
            inactive_count: inactive row count
    """
    if not tickers:
        return TickerSyncResult(upserted_count=0, inactive_count=0)

    # 비활성 처리를 upsert보다 먼저 한다. 종목코드가 재사용된 경우 옛 행이 활성인 채로
    # 남아 있으면 같은 ticker을 쓰는 새 행 insert가 활성 종목 부분 유니크에 걸린다.
    markets = sorted({ticker.market for ticker in tickers})
    active_standard_codes = sorted({ticker.standard_code for ticker in tickers})
    result = session.execute(
        DEACTIVATE_MISSING_TICKERS_SQL,
        {"markets": markets, "active_standard_codes": active_standard_codes},
    )

    payload = [_to_payload(ticker) for ticker in tickers]
    session.execute(UPSERT_TICKER_SQL, payload)

    return TickerSyncResult(
        upserted_count=len(payload),
        inactive_count=result.rowcount or 0,
    )


def _to_payload(ticker: StockTicker) -> dict[str, object]:
    """SQL문에 사용할 dict 자료형으로 변환합니다.

    Args:
        ticker (StockTicker): 주식 정보

    Returns:
        dict: stock dict
    """
    return {
        "ticker": ticker.ticker,
        "standard_code": ticker.standard_code,
        "name": ticker.name,
        "market": ticker.market,
        "security_group": ticker.security_group,
        "listed_date": ticker.listed_date,
        "is_active": ticker.is_active,
        "trading_suspended": ticker.trading_suspended,
        "under_administration": ticker.under_administration,
        "delisting_trade": ticker.delisting_trade,
        "preferred_stock": ticker.preferred_stock,
        "etp": ticker.etp,
        "spac": ticker.spac,
        "krx100": ticker.krx100,
        "krx300": ticker.krx300,
        "kosdaq150": ticker.kosdaq150,
        "listed_shares": ticker.listed_shares,
        "par_value": ticker.par_value,
        "capital": ticker.capital,
        "source": ticker.source,
        "synced_at": ticker.synced_at,
        "inactive_at": ticker.inactive_at,
        "raw_attributes": (
            json.dumps(ticker.raw_attributes, ensure_ascii=False)
            if ticker.raw_attributes is not None
            else None
        ),
    }
