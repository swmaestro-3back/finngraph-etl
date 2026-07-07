from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from zipfile import ZipFile

from pipelines.common.time import KST, now_kst
from pipelines.stocks.models import StockSymbol


KOSPI_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KOSDAQ_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

KOSPI_WIDTHS = [
    2, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12,
    12, 8, 15, 21, 2, 7, 1, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1, 1,
]

KOSDAQ_WIDTHS = [
    2, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21,
    2, 7, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1, 1,
]


@dataclass(frozen=True)
class MarketMasterSpec:
    """KIS 시장별 master 파일 파싱 설정.

    Attributes:
        market (str): 시장 구분. "KOSPI" 또는 "KOSDAQ".
        url (str): KIS master zip 다운로드 URL.
        suffix_widths (list[int]): row 뒤쪽 고정폭 필드들의 길이 목록.
        trading_suspended_index (int): fields 배열에서 거래정지 여부 필드 위치.
        delisting_trade_index (int): fields 배열에서 정리매매 여부 필드 위치.
        under_administration_index (int): fields 배열에서 관리종목 여부 필드 위치.
        listed_date_index (int): fields 배열에서 상장일자 필드 위치.
        preferred_stock_index (int): fields 배열에서 우선주 여부 필드 위치.
        etp_index (int): fields 배열에서 ETP/ETF/ETN 여부 필드 위치.
        spac_index (int): fields 배열에서 SPAC 여부 필드 위치.
    """

    market: str
    url: str
    suffix_widths: list[int]
    trading_suspended_index: int
    delisting_trade_index: int
    under_administration_index: int
    listed_date_index: int
    preferred_stock_index: int
    etp_index: int
    spac_index: int

    @property
    def suffix_length(self) -> int:
        return sum(self.suffix_widths)


KOSPI_SPEC = MarketMasterSpec(
    market="KOSPI",
    url=KOSPI_MASTER_URL,
    suffix_widths=KOSPI_WIDTHS,
    trading_suspended_index=34,
    delisting_trade_index=35,
    under_administration_index=36,
    listed_date_index=49,
    preferred_stock_index=54,
    etp_index=12,
    spac_index=19,
)

KOSDAQ_SPEC = MarketMasterSpec(
    market="KOSDAQ",
    url=KOSDAQ_MASTER_URL,
    suffix_widths=KOSDAQ_WIDTHS,
    trading_suspended_index=29,
    delisting_trade_index=30,
    under_administration_index=31,
    listed_date_index=44,
    preferred_stock_index=49,
    etp_index=8,
    spac_index=14,
)


def fetch_kospi_kosdaq_symbols() -> list[StockSymbol]:
    return fetch_stock_master_symbols((KOSPI_SPEC, KOSDAQ_SPEC))


def fetch_stock_master_symbols(specs: tuple[MarketMasterSpec, ...]) -> list[StockSymbol]:
    synced_at = now_kst()
    symbols: list[StockSymbol] = []
    for spec in specs:
        content = _download_master_file(spec.url)
        symbols.extend(parse_master_content(content, spec, synced_at=synced_at))
    return symbols


def parse_master_content(
    content: bytes,
    spec: MarketMasterSpec,
    synced_at: datetime | None = None,
) -> list[StockSymbol]:
    text = content.decode("cp949")
    return [
        parse_master_row(row, spec, synced_at=synced_at)
        for row in text.splitlines()
        if row.strip()
    ]


def parse_master_row(
    row: str,
    spec: MarketMasterSpec,
    synced_at: datetime | None = None,
) -> StockSymbol:
    suffix_length = spec.suffix_length
    if len(row) < suffix_length + 21:
        raise ValueError(
            f"{spec.market} master row is too short: row_length={len(row)} "
            f"suffix_length={suffix_length}"
        )

    part1 = row[: len(row) - suffix_length]
    fields = _split_fixed_width(row[-suffix_length:], spec.suffix_widths)

    symbol = part1[:9].strip().zfill(6)
    standard_code = part1[9:21].strip()
    name = part1[21:].strip()
    if not symbol or not standard_code or not name:
        raise ValueError(f"{spec.market} master row missing required symbol fields.")

    return StockSymbol(
        symbol=symbol,
        standard_code=standard_code,
        name=name,
        market=spec.market,
        is_active=True,
        listed_date=_parse_date(fields[spec.listed_date_index]),
        trading_suspended=_is_flagged(fields[spec.trading_suspended_index]),
        under_administration=_is_flagged(fields[spec.under_administration_index]),
        delisting_trade=_is_flagged(fields[spec.delisting_trade_index]),
        preferred_stock=_is_flagged(fields[spec.preferred_stock_index]),
        etp=_is_flagged(fields[spec.etp_index]),
        spac=_is_flagged(fields[spec.spac_index]),
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
