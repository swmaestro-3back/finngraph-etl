"""투자자 수급 job의 보유 스냅샷 결합 유닛 테스트.

순매수는 30영업일치가 한 번에 오지만 외국인 보유는 현재 시점 스냅샷 하나뿐이다. 이 값을
어느 행에 붙이느냐가 데이터 정확성을 가른다 — 과거 행에 같은 값을 채우면 없는 이력을
지어내게 된다.
"""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from pipelines.stocks.jobs.collect_investor_flows import _attach_holding
from pipelines.stocks.models import ForeignHolding, InvestorFlow

HOLDING = ForeignHolding(
    ticker="005930",
    hold_qty=2724356859,
    listed_shares=5846278608,
    ratio=Decimal("46.6000"),
)


def _flow(day: int) -> InvestorFlow:
    return InvestorFlow(ticker="005930", trade_date=date(2026, 8, day), foreign_net=100)


class AttachHoldingTest(unittest.TestCase):
    def test_only_latest_date_gets_the_snapshot(self) -> None:
        flows = [_flow(5), _flow(6), _flow(7)]

        result = _attach_holding(flows, HOLDING)

        by_date = {flow.trade_date: flow for flow in result}
        self.assertEqual(by_date[date(2026, 8, 7)].foreign_ratio, HOLDING.ratio)
        self.assertIsNone(by_date[date(2026, 8, 5)].foreign_ratio)

    def test_input_order_does_not_matter(self) -> None:
        """응답이 최신순으로 와도 가장 늦은 날짜를 고른다."""

        flows = [_flow(7), _flow(5), _flow(6)]

        result = _attach_holding(flows, HOLDING)

        filled = [flow for flow in result if flow.foreign_ratio is not None]
        self.assertEqual(len(filled), 1)
        self.assertEqual(filled[0].trade_date, date(2026, 8, 7))

    def test_other_fields_are_preserved(self) -> None:
        result = _attach_holding([_flow(7)], HOLDING)

        self.assertEqual(result[0].foreign_net, 100)
        self.assertEqual(result[0].ticker, "005930")

    def test_single_row_gets_the_snapshot(self) -> None:
        result = _attach_holding([_flow(7)], HOLDING)

        self.assertEqual(result[0].foreign_ratio, Decimal("46.6000"))


if __name__ == "__main__":
    unittest.main()
