"""KIS 재무 응답 정규화 유닛 테스트.

원본을 그대로 넣으면 조용히 틀리는 세 가지를 막는지 본다.
단위(억원→원), sentinel(99.99), 연간 시리즈에 섞인 최신 분기.
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from pipelines.companies.models import CompanyFinancial
from pipelines.companies.transformers.financials import (
    KIS_AMOUNT_UNIT,
    PERIOD_QUARTER,
    PERIOD_QUARTER_CUMULATIVE,
    build_kis_financials,
    build_quarterly_deltas,
    infer_fiscal_month,
)


class KisFinancialTransformerTest(unittest.TestCase):
    def test_amounts_are_converted_from_hundred_millions(self) -> None:
        """KIS 금액은 억원이라 원으로 바꿔 저장한다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="Q",
            balance_rows=[{"stac_yymm": "202512", "total_aset": "5669421.00"}],
            income_rows=[{"stac_yymm": "202512", "sale_account": "3336059.00"}],
            ratio_rows=[],
        )

        self.assertEqual(len(financials), 1)
        self.assertEqual(financials[0].total_assets, 5669421 * KIS_AMOUNT_UNIT)
        self.assertEqual(financials[0].revenue, 3336059 * KIS_AMOUNT_UNIT)

    def test_sentinel_becomes_none(self) -> None:
        """99.99는 값이 아니라 '없음' 표시다. 그대로 저장하면 오염된다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="Q",
            balance_rows=[{"stac_yymm": "202512", "total_aset": "99.99", "total_cptl": "100.00"}],
            income_rows=[],
            ratio_rows=[],
        )

        self.assertIsNone(financials[0].total_assets)
        self.assertEqual(financials[0].total_equity, 100 * KIS_AMOUNT_UNIT)

    def test_annual_series_keeps_only_fiscal_year_ends(self) -> None:
        """연간 조회의 첫 행은 결산월이 아닌 최신 분기라 버려야 한다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="A",
            balance_rows=[
                {"stac_yymm": "202603", "total_aset": "6333396.00"},
                {"stac_yymm": "202512", "total_aset": "5669421.00"},
                {"stac_yymm": "202412", "total_aset": "5000000.00"},
            ],
            income_rows=[],
            ratio_rows=[],
        )

        self.assertEqual([item.fiscal_yymm for item in financials], ["202412", "202512"])

    def test_annual_series_follows_non_december_fiscal_month(self) -> None:
        """3월 결산 법인은 03이 결산월이다. 12월로 박아두면 데이터가 통째로 사라진다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="A",
            balance_rows=[
                {"stac_yymm": "202512", "total_aset": "100.00"},
                {"stac_yymm": "202503", "total_aset": "200.00"},
                {"stac_yymm": "202403", "total_aset": "300.00"},
            ],
            income_rows=[],
            ratio_rows=[],
        )

        self.assertEqual([item.fiscal_yymm for item in financials], ["202403", "202503"])

    def test_quarterly_series_keeps_all_periods(self) -> None:
        """분기 시리즈는 걸러내지 않는다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="Q",
            balance_rows=[
                {"stac_yymm": "202603", "total_aset": "1.00"},
                {"stac_yymm": "202512", "total_aset": "2.00"},
                {"stac_yymm": "202509", "total_aset": "3.00"},
            ],
            income_rows=[],
            ratio_rows=[],
        )

        self.assertEqual(len(financials), 3)

    def test_ratio_values_are_not_unit_converted(self) -> None:
        """ROE·EPS·BPS는 비율·주당 값이라 억원 변환 대상이 아니다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="Q",
            balance_rows=[],
            income_rows=[],
            ratio_rows=[{"stac_yymm": "202512", "roe_val": "10.85", "eps": "6564.00"}],
        )

        self.assertEqual(financials[0].roe, Decimal("10.85"))
        self.assertEqual(financials[0].eps, Decimal("6564.00"))

    def test_periods_without_any_value_are_dropped(self) -> None:
        """세 응답 모두 비어 있는 기간은 행을 만들지 않는다."""
        financials = build_kis_financials(
            company_id=1,
            period_type="Q",
            balance_rows=[{"stac_yymm": "202512"}],
            income_rows=[],
            ratio_rows=[],
        )

        self.assertEqual(financials, [])


if __name__ == "__main__":
    unittest.main()


def _annual(*fiscal_yymms: str) -> list[CompanyFinancial]:
    return [
        CompanyFinancial(company_id=1, source="KIS", fiscal_yymm=y, period_type="A")
        for y in fiscal_yymms
    ]


def _cumulative(fiscal_yymm: str, revenue: int, eps: str, equity: int = 900) -> CompanyFinancial:
    return CompanyFinancial(
        company_id=1,
        source="KIS",
        fiscal_yymm=fiscal_yymm,
        period_type=PERIOD_QUARTER_CUMULATIVE,
        revenue=revenue,
        net_income=revenue // 10,
        eps=Decimal(eps),
        total_equity=equity,
        bps=Decimal("1000"),
        roe=Decimal("5.5"),
    )


# 유한양행 2025년 실측 누적값(억원). 매출이 커지다 새 회계연도에 떨어지는 것이 누적의 증거다.
YUHAN_2025 = [
    _cumulative("202503", 4916, "108"),
    _cumulative("202506", 10706, "668"),
    _cumulative("202509", 16406, "980"),
    _cumulative("202512", 21866, "2389"),
]


class QuarterlyDeltaTest(unittest.TestCase):
    def test_first_quarter_of_the_fiscal_year_is_not_subtracted(self) -> None:
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertEqual(deltas[0].fiscal_yymm, "202503")
        self.assertEqual(deltas[0].revenue, 4916, "1분기 누적은 곧 1분기 개별이다")

    def test_later_quarters_subtract_the_previous_cumulative(self) -> None:
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertEqual([d.revenue for d in deltas], [4916, 5790, 5700, 5460])
        self.assertEqual([d.eps for d in deltas], [Decimal(v) for v in (108, 560, 312, 1409)])

    def test_quarterly_sum_equals_the_annual_figure(self) -> None:
        """회계연도 개별 합 = 연간. 차분이 맞는지 보는 가장 강한 검사다."""
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertEqual(sum(d.revenue for d in deltas), 21866)
        self.assertEqual(sum(d.eps for d in deltas), Decimal("2389"))

    def test_balance_items_are_copied_not_subtracted(self) -> None:
        """자산·부채·자본과 BPS는 시점 잔액이라 차감하면 증감액이 되어 버린다."""
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertTrue(all(d.total_equity == 900 for d in deltas))
        self.assertTrue(all(d.bps == Decimal("1000") for d in deltas))

    def test_ratio_is_cleared(self) -> None:
        """ROE는 차감이 성립하지 않는다. 틀린 값을 남기느니 비운다."""
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertTrue(all(d.roe is None for d in deltas))

    def test_period_type_is_quarter(self) -> None:
        deltas = build_quarterly_deltas(YUHAN_2025, "12")

        self.assertTrue(all(d.period_type == PERIOD_QUARTER for d in deltas))

    def test_missing_previous_quarter_produces_no_row(self) -> None:
        """직전 분기가 없으면 만들지 않는다 — 틀린 값보다 없는 값이 낫다."""
        deltas = build_quarterly_deltas([YUHAN_2025[0], YUHAN_2025[2]], "12")

        self.assertEqual([d.fiscal_yymm for d in deltas], ["202503"])

    def test_march_fiscal_year_starts_at_june(self) -> None:
        """3월 결산이면 6월이 첫 분기다. 03/06/09/12를 상수로 박으면 틀린다."""
        deltas = build_quarterly_deltas(YUHAN_2025, "03")

        self.assertEqual(deltas[0].fiscal_yymm, "202506")
        self.assertEqual(deltas[0].revenue, 10706, "첫 분기라 차감하지 않는다")

    def test_november_fiscal_year_starts_at_february(self) -> None:
        """11월 결산 법인은 02/05/08/11 시리즈를 쓴다. 실재한다."""
        rows = [
            _cumulative("202502", 100, "10"),
            _cumulative("202505", 250, "25"),
        ]
        deltas = build_quarterly_deltas(rows, "11")

        self.assertEqual(
            [(d.fiscal_yymm, d.revenue) for d in deltas], [("202502", 100), ("202505", 150)]
        )


class FiscalMonthInferenceTest(unittest.TestCase):
    def test_fiscal_month_comes_from_the_annual_series(self) -> None:
        self.assertEqual(infer_fiscal_month(_annual("202312", "202412", "202512")), "12")

    def test_non_december_fiscal_month(self) -> None:
        self.assertEqual(infer_fiscal_month(_annual("202403", "202503", "202603")), "03")

    def test_quarterly_series_would_mislead(self) -> None:
        """카카오(12월 결산) 분기 시리즈는 03이 최빈이라 3월 결산으로 오판된다.

        시리즈가 2018-03에 시작해 2026-03에 끝나 03이 하나 더 많기 때문이다.
        그래서 결산월은 반드시 연간 시리즈에서 뽑는다.
        """
        quarterly_months = _annual(
            "202403",
            "202406",
            "202409",
            "202412",
            "202503",
            "202506",
            "202509",
            "202512",
            "202603",
        )
        self.assertEqual(infer_fiscal_month(quarterly_months), "03", "분기로 뽑으면 이렇게 틀린다")
        self.assertEqual(infer_fiscal_month(_annual("202412", "202512")), "12", "연간은 정확하다")

    def test_empty_series_gives_none(self) -> None:
        self.assertIsNone(infer_fiscal_month([]))


class SourceContaminationTest(unittest.TestCase):
    """KIS 원본에 섞여 오는 껍데기·이상값을 적재 단계에서 걸러낸다."""

    @staticmethod
    def _build(yymm: str, balance: dict, income: dict, ratio: dict):
        return build_kis_financials(
            company_id=1,
            period_type="A",
            balance_rows=[{"stac_yymm": yymm, **balance}],
            income_rows=[{"stac_yymm": yymm, **income}],
            ratio_rows=[{"stac_yymm": yymm, **ratio}],
        )

    def test_all_zero_period_produces_no_row(self) -> None:
        """KIS는 보유하지 않는 기간에도 전부 0.00인 행을 만들어 준다.

        주성엔지니어링 2004~2008(API 소급 한계) · 에코프로비엠 2016~2019(상장 전후)가
        그렇다. **DART에도 없어** 폴백으로 메울 수 없다. 0을 저장하면 화면에
        "그 해 매출 0원"으로 찍히는데 사실은 "모른다"다.
        """
        rows = self._build(
            "200412",
            {"total_aset": "0.00", "total_lblt": "0.00", "total_cptl": "0.00"},
            {"sale_account": "0.00", "bsop_prti": "0.00", "thtr_ntin": "0.00"},
            {"eps": "0.00", "bps": "0.00", "roe_val": "0.00"},
        )

        self.assertEqual(rows, [])

    def test_zero_operating_income_is_kept(self) -> None:
        """적자 기업의 영업이익 0은 사실이다. 자산이 살아 있으면 껍데기가 아니다."""
        rows = self._build(
            "202512",
            {"total_aset": "3675.00", "total_lblt": "1995.00", "total_cptl": "1681.00"},
            {"sale_account": "1704.00", "bsop_prti": "0.00", "thtr_ntin": "-20.00"},
            {"eps": "-57.00", "bps": "4692.00", "roe_val": "-3.4"},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].operating_income, 0)

    def test_unrealistic_per_share_values_are_cleared(self) -> None:
        """남광토건 2010년 EPS는 -143,841,822원(주당 -1.4억)이다.

        워크아웃·감자를 겪은 건설사에서 나타나고 2016년 이후는 정상이다. KIS 원본이
        그런 것이라 파싱은 정확하다 — 적재 단계에서 비운다. 금액 필드는 살린다.
        """
        rows = self._build(
            "201012",
            {"total_aset": "5000.00"},
            {"sale_account": "100.00"},
            {"eps": "-143841822.00", "bps": "74358429.00"},
        )

        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].eps)
        self.assertIsNone(rows[0].bps)
        self.assertEqual(rows[0].revenue, 100 * KIS_AMOUNT_UNIT, "금액은 그대로 둔다")

    def test_normal_per_share_values_survive(self) -> None:
        rows = self._build(
            "202512",
            {"total_aset": "5000.00"},
            {"sale_account": "100.00"},
            {"eps": "6993.00", "bps": "71907.00"},
        )

        self.assertEqual(rows[0].eps, Decimal("6993.00"))
        self.assertEqual(rows[0].bps, Decimal("71907.00"))

    def test_zero_roe_is_treated_as_missing(self) -> None:
        """KIS는 과거 구간에서 ROE만 0.00으로 준다. 다른 값은 멀쩡하다.

        SK하이닉스 2004는 순이익 17,213억 · 자본 44,836억이라 실제 ROE가 38% 수준인데
        roe_val이 0.00으로 온다. 전 필드가 0인 껍데기와 달리 자산 기준 필터에 안 걸려
        따로 비운다. **직접 계산해 채우지 않는다** — KIS 산식(평균자본 추정)과 달라
        한 컬럼에 두 정의가 섞이면 기간 간 비교가 깨진다.
        """
        rows = self._build(
            "200412",
            {"total_aset": "44836.00", "total_cptl": "44836.00"},
            {"thtr_ntin": "17213.00"},
            {"eps": "3871.00", "bps": "10000.00", "roe_val": "0.00"},
        )

        self.assertIsNone(rows[0].roe)
        self.assertEqual(rows[0].eps, Decimal("3871.00"), "다른 지표는 살린다")
        self.assertEqual(rows[0].net_income, 17213 * KIS_AMOUNT_UNIT)

    def test_normal_roe_survives(self) -> None:
        rows = self._build(
            "202512", {"total_aset": "100.00"}, {"thtr_ntin": "10.00"}, {"roe_val": "44.15"}
        )

        self.assertEqual(rows[0].roe, Decimal("44.15"))
