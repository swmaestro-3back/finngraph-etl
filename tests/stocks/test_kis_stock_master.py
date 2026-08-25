from __future__ import annotations

import unittest
from datetime import datetime

from pipelines.common.utils.time import KST
from pipelines.stocks.extractors.kis_stock_master import (
    KOSDAQ_SPEC,
    KOSPI_SPEC,
    LISTED_SHARES_UNIT,
    is_collectible,
    parse_master_row,
)
from pipelines.stocks.models import StockTicker


class KisStockMasterTest(unittest.TestCase):
    def test_parse_kospi_master_row(self) -> None:
        """KOSPI 데이터 파싱 테스트"""
        row = _build_row(
            KOSPI_SPEC,
            ticker="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={
                KOSPI_SPEC.security_group_index: "ST",
                KOSPI_SPEC.listed_date_index: "19750611",
                KOSPI_SPEC.trading_suspended_index: "N",
                KOSPI_SPEC.delisting_trade_index: "N",
                KOSPI_SPEC.under_administration_index: "N",
                KOSPI_SPEC.preferred_stock_index: "0",
                KOSPI_SPEC.etp_index: "N",
                KOSPI_SPEC.spac_index: "N",
                KOSPI_SPEC.listed_shares_index: "000000005846278",
                KOSPI_SPEC.par_value_index: "000000000100",
                KOSPI_SPEC.capital_index: "000000000778046685000",
            },
        )

        ticker = parse_master_row(row, KOSPI_SPEC, synced_at=datetime(2026, 7, 7, tzinfo=KST))

        self.assertEqual(ticker.ticker, "005930")
        self.assertEqual(ticker.standard_code, "KR7005930003")
        self.assertEqual(ticker.name, "Samsung")
        self.assertEqual(ticker.market, "KOSPI")
        self.assertEqual(ticker.security_group, "ST")
        listed_date = ticker.listed_date.isoformat() if ticker.listed_date else None
        self.assertEqual(listed_date, "1975-06-11")
        self.assertFalse(ticker.trading_suspended)
        self.assertFalse(ticker.under_administration)
        self.assertFalse(ticker.delisting_trade)
        self.assertFalse(ticker.preferred_stock)
        self.assertFalse(ticker.etp)
        self.assertFalse(ticker.spac)
        # master 상장주수는 천주 단위 -> 주 단위로 정규화되어야 한다
        self.assertEqual(ticker.listed_shares, 5_846_278 * LISTED_SHARES_UNIT)
        self.assertEqual(ticker.par_value, 100)
        self.assertEqual(ticker.capital, 778_046_685_000)

    def test_listed_shares_normalized_to_share_unit(self) -> None:
        """상장주수 단위 변환 누락은 시가총액을 1000배 틀리게 만든다."""
        row = _build_row(
            KOSPI_SPEC,
            ticker="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={KOSPI_SPEC.listed_shares_index: "000000005846278"},
        )

        ticker = parse_master_row(row, KOSPI_SPEC)

        self.assertEqual(ticker.listed_shares, 5_846_278_000)
        # KIS inquire-price의 lstn_stcn(5,846,278,608)과 천주 절삭분만큼만 차이나야 한다
        self.assertLess(abs(ticker.listed_shares - 5_846_278_608), LISTED_SHARES_UNIT)

    def test_parse_master_row_allows_blank_numeric_fields(self) -> None:
        """신규 상장 등으로 숫자 필드가 비어 있어도 파싱은 성공해야 한다."""
        row = _build_row(
            KOSPI_SPEC,
            ticker="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={},
        )

        ticker = parse_master_row(row, KOSPI_SPEC)

        self.assertIsNone(ticker.listed_shares)
        self.assertIsNone(ticker.par_value)
        self.assertIsNone(ticker.capital)

    def test_parse_kosdaq_master_row_flags(self) -> None:
        """KOSDAQ 데이터 파싱 테스트"""
        row = _build_row(
            KOSDAQ_SPEC,
            ticker="091990",
            standard_code="KR7091990002",
            name="CelltrionHC",
            values={
                KOSDAQ_SPEC.listed_date_index: "20050719",
                KOSDAQ_SPEC.trading_suspended_index: "Y",
                KOSDAQ_SPEC.delisting_trade_index: "N",
                KOSDAQ_SPEC.under_administration_index: "Y",
                KOSDAQ_SPEC.preferred_stock_index: "1",
                KOSDAQ_SPEC.etp_index: "0",
                KOSDAQ_SPEC.spac_index: "Y",
            },
        )

        ticker = parse_master_row(row, KOSDAQ_SPEC)

        self.assertEqual(ticker.ticker, "091990")
        self.assertEqual(ticker.market, "KOSDAQ")
        self.assertTrue(ticker.trading_suspended)
        self.assertTrue(ticker.under_administration)
        self.assertFalse(ticker.delisting_trade)
        self.assertTrue(ticker.preferred_stock)
        self.assertFalse(ticker.etp)
        self.assertTrue(ticker.spac)

    def test_parse_master_row_rejects_short_row(self) -> None:
        with self.assertRaisesRegex(ValueError, "row is too short"):
            parse_master_row("too-short", KOSPI_SPEC)


def _build_row(
    spec,
    ticker: str,
    standard_code: str,
    name: str,
    values: dict[int, str],
) -> str:
    fields = ["" for _ in spec.suffix_widths]
    for index, value in values.items():
        fields[index] = value

    suffix = "".join(
        _fit_fixed_width(value, width)
        for value, width in zip(fields, spec.suffix_widths, strict=True)
    )
    return f"{ticker:<9}{standard_code:<12}{name}{suffix}"


def _fit_fixed_width(value: str, width: int) -> str:
    if len(value) > width:
        raise ValueError(f"{value} is wider than {width}")
    return f"{value:<{width}}"


if __name__ == "__main__":
    unittest.main()


class IsCollectibleTest(unittest.TestCase):
    """수집 대상 판정 — 보통주만 들인다."""

    @staticmethod
    def _ticker(**kwargs) -> StockTicker:
        base = {
            "ticker": "005930",
            "standard_code": "KR7005930003",
            "name": "삼성전자",
            "market": "KOSPI",
            "security_group": "ST",
        }
        return StockTicker(**{**base, **kwargs})

    def test_common_stock_passes(self) -> None:
        self.assertTrue(is_collectible(self._ticker()))

    def test_etf_and_etn_are_filtered_by_security_group(self) -> None:
        # ETF·ETN 은 그룹코드가 EF·EN 이라 etp 플래그를 보지 않아도 걸러진다.
        for group in ("EF", "EN"):
            with self.subTest(group=group):
                self.assertFalse(is_collectible(self._ticker(security_group=group, etp=True)))

    def test_non_stock_security_groups_are_filtered(self) -> None:
        # 수익증권·리츠·신주인수권·예탁증서·외국주권은 플래그가 전부 false 다.
        for group in ("BC", "RT", "SR", "SW", "DR", "FS", "IF", "MF"):
            with self.subTest(group=group):
                self.assertFalse(is_collectible(self._ticker(security_group=group)))

    def test_preferred_stock_is_filtered(self) -> None:
        self.assertFalse(is_collectible(self._ticker(preferred_stock=True)))

    def test_spac_is_filtered(self) -> None:
        # 스팩은 법적으로 주식회사여서 ST 로 온다. 그룹코드로는 안 걸러진다.
        self.assertFalse(is_collectible(self._ticker(spac=True)))

    def test_missing_security_group_is_filtered(self) -> None:
        self.assertFalse(is_collectible(self._ticker(security_group=None)))
