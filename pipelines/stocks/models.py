from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

# 증권그룹구분코드 중 주권. 법인 이관 대상을 이 값으로 한정한다.
SECURITY_GROUP_STOCK = "ST"


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
        listed_shares (int | None): 상장주식수. 주 단위로 정규화한 값.
            master 원본은 천주 단위이므로 파싱 단계에서 1000을 곱한다.
            시가총액·EPS·BPS 계산의 분모라 단위를 틀리면 결과가 1000배 어긋난다.
        par_value (int | None): 액면가(원).
        capital (int | None): 자본금(원). 액면가 × 발행주식수인 명목 금액으로,
            자산 − 부채인 자기자본과는 다르다. ROE 분모로 쓰면 안 된다.
        source (str): 종목 정보를 가져온 원천.
        synced_at (datetime | None): 원천 master에서 동기화한 시각.
        inactive_at (datetime | None): 최신 master에서 사라져 비활성 처리한 시각.
        raw_attributes (dict | None): master 파일의 전체 필드를 KIS 공식 이름 키로 보존한 값.
            이력 보존용이 아니다. 매 동기화마다 통째로 덮어쓰므로 최신 스냅샷만 남는다.
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


@dataclass(frozen=True)
class InvestorFlow:
    """종목별 일별 투자자 수급.

    KIS `investor-trade-by-stock-daily`가 101개 필드를 주지만 프론트 스펙이 요구하는
    주체만 컬럼으로 승격한다. 단위는 전부 주(수량)이다.

    순매수는 30영업일치가 오지만 보유율은 현재가 조회 스냅샷이라 최신 한 시점뿐이다.
    그래서 보유율은 수집 회차의 최신 거래일 행에만 채워진다.

    Attributes:
        pension_net (int | None): 연기금(기금, `fund_ntby_qty`) 순매수. 기관계에 포함된
            하위 주체라 institution_net과 더하면 이중 계산이 된다.
        foreign_ratio (Decimal | None): 외국인 보유율(%). 보유주식수 ÷ 상장주식수다.
            KIS의 `hts_frgn_ehrt`는 외국인 한도로 나눈 소진율이라 쓰지 않는다.
    """

    ticker: str
    trade_date: date
    foreign_net: int | None = None
    personal_net: int | None = None
    institution_net: int | None = None
    pension_net: int | None = None
    trust_net: int | None = None
    insurance_net: int | None = None
    bank_net: int | None = None
    etc_corp_net: int | None = None
    foreign_ratio: Decimal | None = None


@dataclass(frozen=True)
class ForeignHolding:
    """외국인 보유 스냅샷.

    현재가 조회가 주는 현재 시점 한 벌이다. 과거 소급이 안 되므로 매일 받아 쌓는다.

    Attributes:
        hold_qty (int): 외국인 보유주식수(`frgn_hldn_qty`).
        listed_shares (int): 조회 시점 상장주식수(`lstn_stcn`). 비율의 분모다.
        ratio (Decimal): 보유율(%). hold_qty ÷ listed_shares × 100.
    """

    ticker: str
    hold_qty: int
    listed_shares: int
    ratio: Decimal


@dataclass(frozen=True)
class Dividend:
    """배당.

    Attributes:
        record_date (date): 배당기준일. 이 날 주주명부에 있어야 배당을 받는다.
        divi_kind (str): '분기' / '결산' 등. 같은 기준일에 종류가 다른 배당이 있을 수 있어
            키에 포함한다.
        dps (Decimal | None): 주당 배당금(원).
        pay_date (date | None): 지급일. 미확정이면 비어 온다.
    """

    ticker: str
    record_date: date
    divi_kind: str
    dps: Decimal | None = None
    pay_date: date | None = None
