from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class StockTicker:
    """KOSPI/KOSDAQ 종목 마스터 데이터.

    Attributes:
        ticker (str): 단축코드. 예: "005930".
        standard_code (str): 표준코드. KIS master의 표준코드 값.
        name (str): 종목명.
        market (str): 시장 구분. "KOSPI" 또는 "KOSDAQ".
        security_group (str | None): 증권그룹구분코드. 주권은 "ST"이고 수익증권("BC"),
            리츠("RT"), 신주인수권("SR"/"SW"), 예탁증서("DR"), 외국주권("FS") 등이 따로 온다.
            ETF/ETN은 etp 플래그로도 걸러지지만 그 밖의 비(非)주권은 이 코드로만 구분된다.
            법인 이관 대상을 "ST"로 한정하는 데 쓴다.
        is_active (bool): 현재 수집 대상 master에 존재하는지 여부.
        listed_date (date | None): 상장일.
        trading_suspended (bool): 거래정지 여부.
        under_administration (bool): 관리종목 여부.
        delisting_trade (bool): 정리매매 여부.
        preferred_stock (bool): 우선주 여부.
        etp (bool): ETP/ETF/ETN 등 상품성 종목 여부.
        spac (bool): SPAC 여부.
        listed_shares (int | None): 상장주식수. **주 단위로 정규화한 값**.
            master 원본은 천주 단위이므로 파싱 단계에서 1000을 곱한다.
            시가총액·EPS·BPS 계산의 분모라 단위를 틀리면 결과가 1000배 어긋난다.
        par_value (int | None): 액면가(원).
        capital (int | None): 자본금(원). 액면가 × 발행주식수인 명목 금액으로,
            자산 − 부채인 자기자본과는 다르다. ROE 분모로 쓰면 안 된다.
        source (str): 종목 정보를 가져온 원천.
        synced_at (datetime | None): 원천 master에서 동기화한 시각.
        inactive_at (datetime | None): 최신 master에서 사라져 비활성 처리한 시각.
        raw_attributes (dict | None): master 파일의 전체 필드를 KIS 공식 이름 키로 보존한 값.
            **이력 보존용이 아니다.** 매 동기화마다 통째로 덮어쓰므로 최신 스냅샷만 남는다.
            필드 탐색(무엇을 컬럼으로 승격할지 실제 값을 보고 판단)과 파싱 검증
            (오프셋이 밀리면 값이 눈에 띄게 이상해진다)을 위한 것이다.
    """

    ticker: str
    standard_code: str
    name: str
    market: str
    security_group: str | None = None
    is_active: bool = True
    listed_date: date | None = None
    trading_suspended: bool = False
    under_administration: bool = False
    delisting_trade: bool = False
    preferred_stock: bool = False
    etp: bool = False
    spac: bool = False
    listed_shares: int | None = None
    par_value: int | None = None
    capital: int | None = None
    source: str = "KIS_MASTER"
    synced_at: datetime | None = None
    inactive_at: datetime | None = None
    raw_attributes: dict[str, str] | None = None


@dataclass(frozen=True)
class TickerSyncResult:
    """종목 마스터 동기화 결과."""

    upserted_count: int
    inactive_count: int
