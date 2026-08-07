from __future__ import annotations

import unittest
from datetime import datetime

from pipelines.common.time import KST
from pipelines.stocks.extractors.kis_stock_master import (
    KOSDAQ_SPEC,
    KOSPI_SPEC,
    LISTED_SHARES_UNIT,
    parse_master_row,
)


class KisStockMasterTest(unittest.TestCase):
    def test_parse_kospi_master_row(self) -> None:
        """KOSPI 데이터 파싱 테스트"""
        row = _build_row(
            KOSPI_SPEC,
            symbol="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={
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

        symbol = parse_master_row(row, KOSPI_SPEC, synced_at=datetime(2026, 7, 7, tzinfo=KST))

        self.assertEqual(symbol.symbol, "005930")
        self.assertEqual(symbol.standard_code, "KR7005930003")
        self.assertEqual(symbol.name, "Samsung")
        self.assertEqual(symbol.market, "KOSPI")
        listed_date = symbol.listed_date.isoformat() if symbol.listed_date else None
        self.assertEqual(listed_date, "1975-06-11")
        self.assertFalse(symbol.trading_suspended)
        self.assertFalse(symbol.under_administration)
        self.assertFalse(symbol.delisting_trade)
        self.assertFalse(symbol.preferred_stock)
        self.assertFalse(symbol.etp)
        self.assertFalse(symbol.spac)
        # master 상장주수는 천주 단위 -> 주 단위로 정규화되어야 한다
        self.assertEqual(symbol.listed_shares, 5_846_278 * LISTED_SHARES_UNIT)
        self.assertEqual(symbol.par_value, 100)
        self.assertEqual(symbol.capital, 778_046_685_000)

    def test_listed_shares_normalized_to_share_unit(self) -> None:
        """상장주수 단위 변환 누락은 시가총액을 1000배 틀리게 만든다."""
        row = _build_row(
            KOSPI_SPEC,
            symbol="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={KOSPI_SPEC.listed_shares_index: "000000005846278"},
        )

        symbol = parse_master_row(row, KOSPI_SPEC)

        self.assertEqual(symbol.listed_shares, 5_846_278_000)
        # KIS inquire-price의 lstn_stcn(5,846,278,608)과 천주 절삭분만큼만 차이나야 한다
        self.assertLess(abs(symbol.listed_shares - 5_846_278_608), LISTED_SHARES_UNIT)

    def test_parse_master_row_allows_blank_numeric_fields(self) -> None:
        """신규 상장 등으로 숫자 필드가 비어 있어도 파싱은 성공해야 한다."""
        row = _build_row(
            KOSPI_SPEC,
            symbol="005930",
            standard_code="KR7005930003",
            name="Samsung",
            values={},
        )

        symbol = parse_master_row(row, KOSPI_SPEC)

        self.assertIsNone(symbol.listed_shares)
        self.assertIsNone(symbol.par_value)
        self.assertIsNone(symbol.capital)

    def test_parse_kosdaq_master_row_flags(self) -> None:
        """KOSDAQ 데이터 파싱 테스트"""
        row = _build_row(
            KOSDAQ_SPEC,
            symbol="091990",
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

        symbol = parse_master_row(row, KOSDAQ_SPEC)

        self.assertEqual(symbol.symbol, "091990")
        self.assertEqual(symbol.market, "KOSDAQ")
        self.assertTrue(symbol.trading_suspended)
        self.assertTrue(symbol.under_administration)
        self.assertFalse(symbol.delisting_trade)
        self.assertTrue(symbol.preferred_stock)
        self.assertFalse(symbol.etp)
        self.assertTrue(symbol.spac)

    def test_parse_master_row_rejects_short_row(self) -> None:
        with self.assertRaisesRegex(ValueError, "row is too short"):
            parse_master_row("too-short", KOSPI_SPEC)


def _build_row(
    spec,
    symbol: str,
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
    return f"{symbol:<9}{standard_code:<12}{name}{suffix}"


def _fit_fixed_width(value: str, width: int) -> str:
    if len(value) > width:
        raise ValueError(f"{value} is wider than {width}")
    return f"{value:<{width}}"


if __name__ == "__main__":
    unittest.main()
