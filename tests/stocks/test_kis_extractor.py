"""KIS 시세 응답 파싱 유닛 테스트.

네트워크를 타지 않도록 클라이언트를 가짜로 바꿔 끼운다. 검증 대상은 응답 해석이다 —
결측 표현(빈 문자열), 최신순 정렬, 100건 페이징.
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal

from pipelines.stocks.extractors.kis import (
    CHART_MARKET_CODE,
    fetch_daily_candles,
    fetch_foreign_holding,
    fetch_period_candles,
    is_market_open,
)


class FakeKisClient:
    """request 호출을 기록하고 미리 준비한 응답을 순서대로 돌려준다."""

    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []

    def request(self, path, tr_id, params, **kwargs):
        self.calls.append({"path": path, "tr_id": tr_id, "params": params})
        return self._responses.pop(0) if self._responses else {"output2": []}


def _chart_row(
    trade_date: str, close: str, volume: str = "100", prdy_vrss: str | None = None
) -> dict:
    row = {
        "stck_bsop_date": trade_date,
        "stck_oprc": close,
        "stck_hgpr": close,
        "stck_lwpr": close,
        "stck_clpr": close,
        "acml_vol": volume,
        "acml_tr_pbmn": "1000",
    }
    if prdy_vrss is not None:
        row["prdy_vrss"] = prdy_vrss
    return row


class ChartExtractorTest(unittest.TestCase):
    def test_daily_candles_are_sorted_ascending(self) -> None:
        """KIS는 최신순으로 주는데 적재는 과거순이 편하다."""
        client = FakeKisClient(
            [{"output2": [_chart_row("20260807", "231000"), _chart_row("20260806", "230500")]}]
        )

        candles = fetch_daily_candles("005930", date(2026, 8, 1), date(2026, 8, 7), client=client)

        self.assertEqual([candle.trade_date.day for candle in candles], [6, 7])
        self.assertEqual(candles[0].close, Decimal("230500"))
        self.assertEqual(candles[0].trade_value, 1000)

    def test_rows_with_missing_price_are_dropped(self) -> None:
        """결측은 빈 문자열로 온다. 0으로 채우면 수익률이 조용히 틀린다."""
        broken = _chart_row("20260807", "231000")
        broken["stck_clpr"] = ""
        client = FakeKisClient([{"output2": [broken, _chart_row("20260806", "230500")]}])

        candles = fetch_daily_candles("005930", date(2026, 8, 1), date(2026, 8, 7), client=client)

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].trade_date, date(2026, 8, 6))

    def test_period_candles_page_until_range_is_covered(self) -> None:
        """1회 100건 제한이라 더 긴 구간은 커서를 옮겨 다시 부른다.

        가득 찬 한 페이지(100건)를 주면 더 있을 수 있다는 신호이므로, 가장 오래된 날짜
        직전으로 커서를 옮겨 한 번 더 부른다.
        """
        # 2026-08-07부터 하루씩 거슬러 100건. 최신순으로 오는 원본 순서를 그대로 흉내낸다.
        first_page = [
            _chart_row((date(2026, 8, 7) - timedelta(days=offset)).strftime("%Y%m%d"), "100")
            for offset in range(100)
        ]
        second_page = [_chart_row("20250101", "100")]

        client = FakeKisClient([{"output2": first_page}, {"output2": second_page}])

        candles = fetch_period_candles(
            "005930", "W", date(2025, 1, 1), date(2026, 8, 7), client=client
        )

        self.assertEqual(len(client.calls), 2)
        # 두 번째 호출의 종료일은 첫 페이지에서 가장 오래된 날짜의 하루 전이어야 한다.
        self.assertEqual(client.calls[1]["params"]["FID_INPUT_DATE_2"], "20260429")
        self.assertEqual(candles[0].base_date, date(2025, 1, 1))
        self.assertEqual(candles[0].period, "W")

    def test_period_candles_stop_on_short_page(self) -> None:
        """100건 미만이면 더 없다는 뜻이라 한 번만 부른다."""
        client = FakeKisClient([{"output2": [_chart_row("20260807", "231000")]}])

        fetch_period_candles("005930", "W", date(2020, 1, 1), date(2026, 8, 7), client=client)

        self.assertEqual(len(client.calls), 1)

    def test_period_rejects_unknown_code(self) -> None:
        with self.assertRaises(ValueError):
            fetch_period_candles("005930", "D", date(2026, 1, 1), date(2026, 8, 7), client=None)


class MarketCodeTest(unittest.TestCase):
    def test_chart_market_code_is_integrated(self) -> None:
        self.assertEqual(CHART_MARKET_CODE, "UN")

    def test_daily_chart_requests_integrated_market(self) -> None:
        client = FakeKisClient([{"output2": [_chart_row("20261001", "212000")]}])

        fetch_daily_candles("066570", date(2026, 10, 1), date(2026, 10, 1), client=client)

        self.assertEqual(client.calls[0]["params"]["FID_COND_MRKT_DIV_CODE"], "UN")

    def test_period_chart_requests_integrated_market(self) -> None:
        client = FakeKisClient([{"output2": [_chart_row("20261001", "212000")]}])

        fetch_period_candles("066570", "W", date(2026, 9, 1), date(2026, 10, 1), client=client)

        self.assertEqual(client.calls[0]["params"]["FID_COND_MRKT_DIV_CODE"], "UN")

    def test_foreign_holding_keeps_krx_market(self) -> None:
        client = FakeKisClient([{"output": {"frgn_hldn_qty": "10", "lstn_stcn": "100"}}])

        fetch_foreign_holding("066570", client=client)

        self.assertEqual(client.calls[0]["params"]["FID_COND_MRKT_DIV_CODE"], "J")


class BasePriceTest(unittest.TestCase):
    def _base_price(self, close: str, prdy_vrss: str | None) -> Decimal | None:
        client = FakeKisClient([{"output2": [_chart_row("20261001", close, prdy_vrss=prdy_vrss)]}])
        candles = fetch_daily_candles("066570", date(2026, 10, 1), date(2026, 10, 1), client=client)
        self.assertEqual(len(candles), 1)
        return candles[0].base_price

    def test_base_price_is_close_minus_prdy_vrss(self) -> None:
        self.assertEqual(self._base_price("212000", "-500"), Decimal("212500"))
        self.assertEqual(self._base_price("215500", "4000"), Decimal("211500"))

    def test_base_price_equals_close_when_unchanged(self) -> None:
        self.assertEqual(self._base_price("213500", "0"), Decimal("213500"))

    def test_base_price_is_none_without_prdy_vrss(self) -> None:
        self.assertIsNone(self._base_price("212000", None))
        self.assertIsNone(self._base_price("212000", ""))
        self.assertIsNone(self._base_price("212000", "  "))

    def test_base_price_is_none_when_prdy_vrss_is_unparseable(self) -> None:
        self.assertIsNone(self._base_price("212000", "abc"))
        self.assertIsNone(self._base_price("212000", "NaN"))

    def test_base_price_is_none_when_not_positive(self) -> None:
        self.assertIsNone(self._base_price("100", "100"))
        self.assertIsNone(self._base_price("100", "150"))

    def test_base_price_keeps_price_limit_boundaries(self) -> None:
        self.assertEqual(self._base_price("13000", "3000"), Decimal("10000"))
        self.assertEqual(self._base_price("7000", "-3000"), Decimal("10000"))

    def test_base_price_is_none_outside_price_limit(self) -> None:
        self.assertIsNone(self._base_price("13001", "3001"))
        self.assertIsNone(self._base_price("6999", "-3001"))
        self.assertIsNone(self._base_price("40000", "30000"))

    def test_period_candles_do_not_carry_base_price(self) -> None:
        client = FakeKisClient([{"output2": [_chart_row("20261001", "212000", prdy_vrss="-500")]}])

        candles = fetch_period_candles(
            "066570", "W", date(2026, 9, 1), date(2026, 10, 1), client=client
        )

        self.assertFalse(hasattr(candles[0], "base_price"))


class MarketOpenTest(unittest.TestCase):
    def test_reads_opnd_yn_for_target_date(self) -> None:
        client = FakeKisClient(
            [
                {
                    "output": [
                        {"bass_dt": "20260925", "opnd_yn": "N"},
                        {"bass_dt": "20260928", "opnd_yn": "Y"},
                    ]
                }
            ]
        )

        self.assertFalse(is_market_open(date(2026, 9, 25), client=client))
        self.assertEqual(client.calls[0]["params"]["BASS_DT"], "20260925")

    def test_open_day(self) -> None:
        client = FakeKisClient([{"output": [{"bass_dt": "20260928", "opnd_yn": "Y"}]}])

        self.assertTrue(is_market_open(date(2026, 9, 28), client=client))

    def test_missing_date_raises(self) -> None:
        client = FakeKisClient([{"output": [{"bass_dt": "20260929", "opnd_yn": "Y"}]}])

        with self.assertRaises(ValueError):
            is_market_open(date(2026, 9, 28), client=client)


if __name__ == "__main__":
    unittest.main()
