from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from zipfile import ZipFile

from pipelines.common.utils.time import now_kst
from pipelines.stocks.models import SECURITY_GROUP_STOCK, StockTicker

KOSPI_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KOSDAQ_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

KOSPI_WIDTHS = [
    2,
    1,
    4,
    4,
    4,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    9,
    5,
    5,
    1,
    1,
    1,
    2,
    1,
    1,
    1,
    2,
    2,
    2,
    3,
    1,
    3,
    12,
    12,
    8,
    15,
    21,
    2,
    7,
    1,
    1,
    1,
    1,
    1,
    9,
    9,
    9,
    5,
    9,
    8,
    9,
    3,
    1,
    1,
    1,
]

KOSDAQ_WIDTHS = [
    2,
    1,
    4,
    4,
    4,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    9,
    5,
    5,
    1,
    1,
    1,
    2,
    1,
    1,
    1,
    2,
    2,
    2,
    3,
    1,
    3,
    12,
    12,
    8,
    15,
    21,
    2,
    7,
    1,
    1,
    1,
    1,
    9,
    9,
    9,
    5,
    9,
    8,
    9,
    3,
    1,
    1,
    1,
]


# KIS 공식 샘플(open-trading-api/stocks_info/kis_kospi_code_mst.py)의 part2 컬럼 순서.
# KOSPI_WIDTHS와 1:1 대응하며, 개수가 어긋나면 parse에서 strict zip이 즉시 실패한다.
KOSPI_FIELD_NAMES = [
    "그룹코드",
    "시가총액규모",
    "지수업종대분류",
    "지수업종중분류",
    "지수업종소분류",
    "제조업",
    "저유동성",
    "지배구조지수종목",
    "KOSPI200섹터업종",
    "KOSPI100",
    "KOSPI50",
    "KRX",
    "ETP",
    "ELW발행",
    "KRX100",
    "KRX자동차",
    "KRX반도체",
    "KRX바이오",
    "KRX은행",
    "SPAC",
    "KRX에너지화학",
    "KRX철강",
    "단기과열",
    "KRX미디어통신",
    "KRX건설",
    "Non1",
    "KRX증권",
    "KRX선박",
    "KRX섹터_보험",
    "KRX섹터_운송",
    "SRI",
    "기준가",
    "매매수량단위",
    "시간외수량단위",
    "거래정지",
    "정리매매",
    "관리종목",
    "시장경고",
    "경고예고",
    "불성실공시",
    "우회상장",
    "락구분",
    "액면변경",
    "증자구분",
    "증거금비율",
    "신용가능",
    "신용기간",
    "전일거래량",
    "액면가",
    "상장일자",
    "상장주수",
    "자본금",
    "결산월",
    "공모가",
    "우선주",
    "공매도과열",
    "이상급등",
    "KRX300",
    "KOSPI",
    "매출액",
    "영업이익",
    "경상이익",
    "당기순이익",
    "ROE",
    "기준년월",
    "시가총액",
    "그룹사코드",
    "회사신용한도초과",
    "담보대출가능",
    "대주가능",
]

# KIS 공식 샘플(open-trading-api/stocks_info/kis_kosdaq_code_mst.py)의 part2 컬럼 순서.
KOSDAQ_FIELD_NAMES = [
    "증권그룹구분코드",
    "시가총액규모",
    "지수업종대분류",
    "지수업종중분류",
    "지수업종소분류",
    "벤처기업여부",
    "저유동성종목여부",
    "KRX종목여부",
    "ETP상품구분코드",
    "KRX100종목여부",
    "KRX자동차여부",
    "KRX반도체여부",
    "KRX바이오여부",
    "KRX은행여부",
    "기업인수목적회사여부",
    "KRX에너지화학여부",
    "KRX철강여부",
    "단기과열종목구분코드",
    "KRX미디어통신여부",
    "KRX건설여부",
    "투자주의환기종목여부",
    "KRX증권구분",
    "KRX선박구분",
    "KRX섹터지수보험여부",
    "KRX섹터지수운송여부",
    "KOSDAQ150지수여부",
    "주식기준가",
    "정규시장매매수량단위",
    "시간외시장매매수량단위",
    "거래정지여부",
    "정리매매여부",
    "관리종목여부",
    "시장경고구분코드",
    "시장경고위험예고여부",
    "불성실공시여부",
    "우회상장여부",
    "락구분코드",
    "액면가변경구분코드",
    "증자구분코드",
    "증거금비율",
    "신용주문가능여부",
    "신용기간",
    "전일거래량",
    "주식액면가",
    "주식상장일자",
    "상장주수",
    "자본금",
    "결산월",
    "공모가격",
    "우선주구분코드",
    "공매도과열종목여부",
    "이상급등종목여부",
    "KRX300종목여부",
    "매출액",
    "영업이익",
    "경상이익",
    "당기순이익",
    "ROE",
    "기준년월",
    "전일기준시가총액",
    "그룹사코드",
    "회사신용한도초과여부",
    "담보대출가능여부",
    "대주가능여부",
]


# master의 상장주수는 천주 단위다. KIS `inquire-price`의 lstn_stcn(주 단위)과 대조해 확인했다.
#   삼성전자    master 5,846,278 ↔ KIS 5,846,278,608
#   SK하이닉스  master   730,492 ↔ KIS   730,492,365
#   카카오      master   442,981 ↔ KIS   442,981,070
# 그대로 저장하면 시가총액·EPS·BPS가 1,000배 어긋나므로 적재 전에 주 단위로 정규화한다.
#
# 다만 master 값은 천주 단위로 절삭되어 있어 최대 999주의 오차가 남는다(위 삼성전자 608주).
# 시가총액 기준 0.00002% 수준이라 화면 표시에는 무해하지만, 정확한 주식수가 필요하면
# KIS inquire-price의 lstn_stcn을 써야 한다.
LISTED_SHARES_UNIT = 1000


@dataclass(frozen=True)
class MarketMasterSpec:
    """KIS 시장별 master 파일 파싱 설정.

    Attributes:
        market (str): 시장 구분. "KOSPI" 또는 "KOSDAQ".
        url (str): KIS master zip 다운로드 URL.
        security_group_index (int): fields 배열에서 증권그룹구분코드 필드 위치.
            KOSPI는 "그룹코드", KOSDAQ은 "증권그룹구분코드"로 이름이 다르지만 둘 다 맨 앞이다.
        suffix_widths (list[int]): row 뒤쪽 고정폭 필드들의 길이 목록.
        field_names (list[str]): suffix_widths와 1:1 대응하는 KIS 공식 필드 이름 목록.
        trading_suspended_index (int): fields 배열에서 거래정지 여부 필드 위치.
        delisting_trade_index (int): fields 배열에서 정리매매 여부 필드 위치.
        under_administration_index (int): fields 배열에서 관리종목 여부 필드 위치.
        listed_date_index (int): fields 배열에서 상장일자 필드 위치.
        preferred_stock_index (int): fields 배열에서 우선주 여부 필드 위치.
        etp_index (int): fields 배열에서 ETP/ETF/ETN 여부 필드 위치.
        spac_index (int): fields 배열에서 SPAC 여부 필드 위치.
        listed_shares_index (int): fields 배열에서 상장주수 필드 위치. 값은 천주 단위다.
        par_value_index (int): fields 배열에서 액면가 필드 위치. 원 단위.
        capital_index (int): fields 배열에서 자본금 필드 위치. 원 단위.
    """

    market: str
    url: str
    security_group_index: int
    suffix_widths: list[int]
    field_names: list[str]
    trading_suspended_index: int
    delisting_trade_index: int
    under_administration_index: int
    listed_date_index: int
    preferred_stock_index: int
    etp_index: int
    spac_index: int
    listed_shares_index: int
    par_value_index: int
    capital_index: int

    @property
    def suffix_length(self) -> int:
        return sum(self.suffix_widths)


KOSPI_SPEC = MarketMasterSpec(
    market="KOSPI",
    url=KOSPI_MASTER_URL,
    security_group_index=0,
    suffix_widths=KOSPI_WIDTHS,
    field_names=KOSPI_FIELD_NAMES,
    trading_suspended_index=34,
    delisting_trade_index=35,
    under_administration_index=36,
    listed_date_index=49,
    preferred_stock_index=54,
    etp_index=12,
    spac_index=19,
    listed_shares_index=50,
    par_value_index=48,
    capital_index=51,
)

KOSDAQ_SPEC = MarketMasterSpec(
    market="KOSDAQ",
    url=KOSDAQ_MASTER_URL,
    security_group_index=0,
    suffix_widths=KOSDAQ_WIDTHS,
    field_names=KOSDAQ_FIELD_NAMES,
    trading_suspended_index=29,
    delisting_trade_index=30,
    under_administration_index=31,
    listed_date_index=44,
    preferred_stock_index=49,
    etp_index=8,
    spac_index=14,
    listed_shares_index=45,
    par_value_index=43,
    capital_index=46,
)


def fetch_kospi_kosdaq_tickers() -> list[StockTicker]:
    return fetch_stock_master_tickers((KOSPI_SPEC, KOSDAQ_SPEC))


def is_collectible(ticker: StockTicker) -> bool:
    """수집 대상인가 — 보통주만.

    ETF·ETN 은 security_group 이 EF·EN 이라 이 조건 하나로 함께 걸러진다. 스팩은 법적으로
    주식회사여서 ST 로 오므로 따로 뺀다(실측 ST 2,721 중 71).

    여기서 거르면 stocks 테이블이 곧 "우리가 다루는 종목"이 되어, 하류가 종류를 다시
    판정할 필요가 없다. 마스터에서 빠진 종목은 다음 동기화에서 is_active 가 내려간다.
    """

    return (
        ticker.security_group == SECURITY_GROUP_STOCK
        and not ticker.preferred_stock
        and not ticker.spac
    )


def fetch_stock_master_tickers(specs: tuple[MarketMasterSpec, ...]) -> list[StockTicker]:
    synced_at = now_kst()
    tickers: list[StockTicker] = []
    for spec in specs:
        content = _download_master_file(spec.url)
        parsed = parse_master_content(content, spec, synced_at=synced_at)
        tickers.extend(ticker for ticker in parsed if is_collectible(ticker))
    return tickers


def parse_master_content(
    content: bytes,
    spec: MarketMasterSpec,
    synced_at: datetime | None = None,
) -> list[StockTicker]:
    text = content.decode("cp949")
    return [
        parse_master_row(row, spec, synced_at=synced_at) for row in text.splitlines() if row.strip()
    ]


def parse_master_row(
    row: str,
    spec: MarketMasterSpec,
    synced_at: datetime | None = None,
) -> StockTicker:
    suffix_length = spec.suffix_length
    if len(row) < suffix_length + 21:
        raise ValueError(
            f"{spec.market} master row is too short: row_length={len(row)} "
            f"suffix_length={suffix_length}"
        )

    part1 = row[: len(row) - suffix_length]
    fields = _split_fixed_width(row[-suffix_length:], spec.suffix_widths)

    ticker = part1[:9].strip().zfill(6)
    standard_code = part1[9:21].strip()
    name = part1[21:].strip()
    if not ticker or not standard_code or not name:
        raise ValueError(f"{spec.market} master row missing required ticker fields.")

    # KIS 공식 필드 이름을 키로 전체 속성을 보존한다(요구사항 미확정 대비 — 컬럼 승격은 추후 판단).
    raw_attributes = dict(zip(spec.field_names, fields, strict=True))

    return StockTicker(
        ticker=ticker,
        standard_code=standard_code,
        name=name,
        market=spec.market,
        security_group=fields[spec.security_group_index].strip().upper() or None,
        is_active=True,
        raw_attributes=raw_attributes,
        listed_date=_parse_date(fields[spec.listed_date_index]),
        trading_suspended=_is_flagged(fields[spec.trading_suspended_index]),
        under_administration=_is_flagged(fields[spec.under_administration_index]),
        delisting_trade=_is_flagged(fields[spec.delisting_trade_index]),
        preferred_stock=_is_flagged(fields[spec.preferred_stock_index]),
        etp=_is_flagged(fields[spec.etp_index]),
        spac=_is_flagged(fields[spec.spac_index]),
        listed_shares=_parse_listed_shares(fields[spec.listed_shares_index]),
        par_value=_parse_int(fields[spec.par_value_index]),
        capital=_parse_int(fields[spec.capital_index]),
        synced_at=synced_at or now_kst(),
    )


def _download_master_file(url: str) -> bytes:
    import requests

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    with ZipFile(BytesIO(response.content)) as archive:
        names = [name for name in archive.namelist() if name.endswith(".mst")]
        if len(names) != 1:
            raise RuntimeError(f"Expected one .mst file in archive, found {names}")
        return archive.read(names[0])


def _split_fixed_width(value: str, widths: list[int]) -> list[str]:
    fields: list[str] = []
    offset = 0
    for width in widths:
        fields.append(value[offset : offset + width].strip())
        offset += width
    if offset != len(value):
        raise ValueError(f"Fixed-width parse consumed {offset} chars from {len(value)} chars.")
    return fields


def _parse_date(value: str) -> date | None:
    stripped = value.strip()
    if len(stripped) != 8 or not stripped.isdigit() or stripped == "00000000":
        return None
    return datetime.strptime(stripped, "%Y%m%d").date()


def _is_flagged(value: str) -> bool:
    return value.strip().upper() not in ("", "0", "N")


def _parse_int(value: str) -> int | None:
    """zero-padding된 숫자 문자열을 int로. 비었거나 숫자가 아니면 None."""
    stripped = value.strip()
    if not stripped or not stripped.isdigit():
        return None
    return int(stripped)


def _parse_listed_shares(value: str) -> int | None:
    """상장주수를 주 단위로 정규화한다. master 원본은 천주 단위(LISTED_SHARES_UNIT)."""
    thousands = _parse_int(value)
    if thousands is None:
        return None
    return thousands * LISTED_SHARES_UNIT
