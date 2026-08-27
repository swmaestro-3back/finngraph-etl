"""네이버 trend 응답 파싱 유닛 테스트.

비공식 API 라 응답 형태가 예고 없이 바뀔 수 있다. 파서가 형태 변경을 조용히 삼키지 않고
즉시 실패하는지, 원천의 표기 편차(float 잡음·부호·쉼표)를 정규화하는지가 관심사다.
"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from pipelines.stocks.extractors.naver import parse_trend_rows

# 2026-08-27 실제 응답에서 가져온 형태. 쓰지 않는 키(closePrice 등)도 그대로 둔다.
SAMPLE_ROW = {
    "itemCode": "005930",
    "bizdate": "20260827",
    "organPureBuyQuant": "-97433",
    "individualPureBuyQuant": "-3223427",
    "foreignerPureBuyQuant": "1381786",
    "frgnStock": "2730850645",
    "frgnHoldRatio": "46.709999084472656",
    "closePrice": "266000",
}


class ParseTrendRowsTest(unittest.TestCase):
    def test_maps_fields_and_normalizes_ratio_noise(self) -> None:
        flows = parse_trend_rows("005930", [SAMPLE_ROW])

        flow = flows[0]
        self.assertEqual(flow.trade_date, date(2026, 8, 27))
        self.assertEqual(flow.individual_net_qty, -3223427)
        self.assertEqual(flow.institution_net_qty, -97433)
        self.assertEqual(flow.foreign_net_qty, 1381786)
        self.assertEqual(flow.foreign_hold_ratio, Decimal("46.7100"))

    def test_sorts_ascending_even_if_response_is_newest_first(self) -> None:
        older = dict(SAMPLE_ROW, bizdate="20260825")
        flows = parse_trend_rows("005930", [SAMPLE_ROW, older])

        self.assertEqual(
            [flow.trade_date for flow in flows], [date(2026, 8, 25), date(2026, 8, 27)]
        )

    def test_empty_value_means_missing_not_error(self) -> None:
        """거래정지 등으로 값이 비어 오는 날은 결측(None)으로 담는다."""

        row = dict(SAMPLE_ROW, foreignerPureBuyQuant="", frgnHoldRatio="")
        flows = parse_trend_rows("005930", [row])

        self.assertIsNone(flows[0].foreign_net_qty)
        self.assertIsNone(flows[0].foreign_hold_ratio)

    def test_unparsable_quantity_fails_loudly(self) -> None:
        """값이 있는데 해석이 안 되면 포맷 변경이다. None으로 삼키면 안 된다."""

        row = dict(SAMPLE_ROW, foreignerPureBuyQuant="+1,381,786")

        with self.assertRaises(ValueError):
            parse_trend_rows("005930", [row])

    def test_unparsable_ratio_fails_loudly(self) -> None:
        row = dict(SAMPLE_ROW, frgnHoldRatio="46.73%")

        with self.assertRaises(ValueError):
            parse_trend_rows("005930", [row])

    def test_missing_required_key_fails_loudly(self) -> None:
        row = {key: value for key, value in SAMPLE_ROW.items() if key != "organPureBuyQuant"}

        with self.assertRaises(KeyError):
            parse_trend_rows("005930", [row])

    def test_non_list_response_fails_loudly(self) -> None:
        with self.assertRaises(ValueError):
            parse_trend_rows("005930", {"error": "blocked"})

    def test_unparsable_bizdate_fails_loudly(self) -> None:
        row = dict(SAMPLE_ROW, bizdate="어제")

        with self.assertRaises(ValueError):
            parse_trend_rows("005930", [row])


if __name__ == "__main__":
    unittest.main()
