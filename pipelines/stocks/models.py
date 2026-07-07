from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class StockSymbol:
    """KOSPI/KOSDAQ 종목 마스터 데이터.

    Attributes:
        symbol (str): 단축코드. 예: "005930".
        standard_code (str): 표준코드. KIS master의 표준코드 값.
        name (str): 종목명.
        market (str): 시장 구분. "KOSPI" 또는 "KOSDAQ".
        is_active (bool): 현재 수집 대상 master에 존재하는지 여부.
        listed_date (date | None): 상장일.
        trading_suspended (bool): 거래정지 여부.
        under_administration (bool): 관리종목 여부.
        delisting_trade (bool): 정리매매 여부.
        preferred_stock (bool): 우선주 여부.
        etp (bool): ETP/ETF/ETN 등 상품성 종목 여부.
        spac (bool): SPAC 여부.
        source (str): 종목 정보를 가져온 원천.
        synced_at (datetime | None): 원천 master에서 동기화한 시각.
        inactive_at (datetime | None): 최신 master에서 사라져 비활성 처리한 시각.
    """

    symbol: str
    standard_code: str
    name: str
    market: str
    is_active: bool = True
    listed_date: date | None = None
    trading_suspended: bool = False
    under_administration: bool = False
    delisting_trade: bool = False
    preferred_stock: bool = False
    etp: bool = False
    spac: bool = False
    source: str = "KIS_MASTER"
    synced_at: datetime | None = None
    inactive_at: datetime | None = None


@dataclass(frozen=True)
class SymbolSyncResult:
    """종목 마스터 동기화 결과."""

    upserted_count: int
    inactive_count: int
