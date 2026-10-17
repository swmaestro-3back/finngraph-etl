"""DART 재무 응답 정규화 유닛 테스트.

한 응답에 재무제표 다섯 개(BS·IS·CIS·CF·SCE)가 함께 오고, 같은 account_id 가 여러 표에
서로 다른 뜻으로 나온다. 표를 가리지 않으면 자본변동표의 증감 한 줄이 자본총계 자리에
들어간다. fs_div 는 응답에 없어 요청한 쪽이 알려줘야 한다.
"""

from __future__ import annotations

import unittest

from pipelines.companies.transformers.dart import (
    build_dart_financials,
    normalize_corp_name,
)


class DartTransformerTest(unittest.TestCase):
    """DART 응답 정규화.

    응답에는 다섯 개 표(BS·IS·CIS·CF·SCE)가 함께 온다. 어느 표에서 온 값인지 가리지
    않으면 자본변동표의 증감 한 줄을 자본총계로 넣게 된다. 그리고 응답에는 fs_div가
    없어서, 연결·별도를 요청한 쪽이 알려줘야 한다.
    """

    @staticmethod
    def _row(account_id: str, sj_div: str, amount: str, **extra) -> dict:
        row = {
            "rcept_no": "20250311001085",
            "bsns_year": "2024",
            "sj_div": sj_div,
            "account_id": account_id,
            "thstrm_amount": amount,
        }
        row.update(extra)
        return row

    def test_normalize_strips_legal_forms_and_brackets(self) -> None:
        """'(주)비바리퍼블리카'와 '비바리퍼블리카'는 같은 법인이다."""
        self.assertEqual(
            normalize_corp_name("(주)비바리퍼블리카"),
            normalize_corp_name("비바리퍼블리카"),
        )
        self.assertEqual(normalize_corp_name("주식회사 카카오"), normalize_corp_name("카카오"))
        self.assertEqual(normalize_corp_name("㈜SK 하이닉스"), normalize_corp_name("SK하이닉스"))

    def test_normalize_does_not_merge_different_companies(self) -> None:
        """부분 문자열이 아니라 정확 일치여야 한다. 컬리와 컬리넌종합건설은 다른 법인이다."""
        self.assertNotEqual(normalize_corp_name("컬리"), normalize_corp_name("컬리넌종합건설"))
        self.assertNotEqual(normalize_corp_name("당근마켓"), normalize_corp_name("당근페이"))

    def test_account_id_mapping(self) -> None:
        """XBRL 표준 계정을 우리 컬럼으로 옮긴다."""
        financials = build_dart_financials(
            company_id=7,
            rows=[
                self._row("ifrs-full_Assets", "BS", "324966127000000"),
                self._row("ifrs-full_Revenue", "IS", "209295000000000"),
            ],
            fs_div="OFS",
            fiscal_month="12",
        )

        self.assertEqual(len(financials), 1)
        self.assertEqual(financials[0].total_assets, 324966127000000)
        self.assertEqual(financials[0].revenue, 209295000000000)
        self.assertEqual(financials[0].fs_div, "OFS")
        self.assertEqual(financials[0].fiscal_yymm, "202412")

    def test_fs_div_comes_from_the_request_not_the_response(self) -> None:
        """응답에는 fs_div가 없다. 요청에 쓴 값을 그대로 기록해야 한다.

        이걸 응답에서 읽으려 하면 항상 비어서 연결·별도가 같은 키로 접히고, 나중에 적재된
        쪽이 앞선 것을 덮어쓴다. 실제로 경방 2025년 연결 매출 4,125억 자리에 별도 매출
        2,675억이 들어가 있었다.
        """

        rows = [self._row("ifrs-full_Revenue", "IS", "267500000000")]

        separate = build_dart_financials(company_id=7, rows=rows, fs_div="OFS")
        consolidated = build_dart_financials(company_id=7, rows=rows, fs_div="CFS")

        self.assertEqual(separate[0].fs_div, "OFS")
        self.assertEqual(consolidated[0].fs_div, "CFS")

    def test_equity_is_read_from_balance_sheet_only(self) -> None:
        """자본변동표(SCE)의 증감 한 줄을 자본총계로 삼으면 안 된다.

        경방 2025년 응답에서 ifrs-full_Equity가 8번 나오는데, BS의 8,118억만 자본총계이고
        SCE의 137억·-209억 등은 배당·기타포괄손익으로 인한 증감 내역이다.
        """

        financials = build_dart_financials(
            company_id=7,
            rows=[
                self._row("ifrs-full_Equity", "SCE", "13700000000"),
                self._row("ifrs-full_Equity", "SCE", "-20900000000"),
                self._row("ifrs-full_Equity", "BS", "811800000000"),
                self._row("ifrs-full_Equity", "SCE", "0"),
            ],
            fs_div="CFS",
        )

        self.assertEqual(financials[0].total_equity, 811800000000)

    def test_statement_order_in_response_does_not_matter(self) -> None:
        """엉뚱한 표가 먼저 와도 결과가 같아야 한다. 응답 순서에 기대지 않는다."""

        correct = self._row("ifrs-full_ProfitLoss", "IS", "47400000000")
        noise = self._row("ifrs-full_ProfitLoss", "CF", "99999999999")

        first = build_dart_financials(company_id=7, rows=[noise, correct], fs_div="CFS")
        second = build_dart_financials(company_id=7, rows=[correct, noise], fs_div="CFS")

        self.assertEqual(first[0].net_income, 47400000000)
        self.assertEqual(second[0].net_income, 47400000000)

    def test_comprehensive_income_is_a_fallback_for_income_statement(self) -> None:
        """IS 없이 포괄손익계산서(CIS)만 제출하는 기업이 있다. 그때는 CIS를 쓴다."""

        only_cis = build_dart_financials(
            company_id=7,
            rows=[self._row("ifrs-full_ProfitLoss", "CIS", "500")],
            fs_div="CFS",
        )
        self.assertEqual(only_cis[0].net_income, 500)

        both = build_dart_financials(
            company_id=7,
            rows=[
                self._row("ifrs-full_ProfitLoss", "CIS", "500"),
                self._row("ifrs-full_ProfitLoss", "IS", "700"),
            ],
            fs_div="CFS",
        )
        self.assertEqual(both[0].net_income, 700, "IS가 있으면 IS를 우선한다")

    def test_cash_flow_statement_is_ignored(self) -> None:
        """현금흐름표에도 당기순이익이 나오지만 손익 항목의 출처가 아니다."""

        financials = build_dart_financials(
            company_id=7,
            rows=[self._row("ifrs-full_ProfitLoss", "CF", "47400000000")],
            fs_div="CFS",
        )

        self.assertEqual(financials, [])

    def test_account_name_fallback(self) -> None:
        """account_id가 비어 오는 3% 행은 계정명으로 잡는다."""
        financials = build_dart_financials(
            company_id=7,
            rows=[
                self._row("", "IS", "6566976000000", account_nm="영업이익"),
            ],
            fs_div="CFS",
        )

        self.assertEqual(financials[0].operating_income, 6566976000000)

    def test_disclosed_at_comes_from_receipt_number(self) -> None:
        """접수번호 앞 8자리가 공시일이다. point-in-time의 근거가 된다."""
        financials = build_dart_financials(
            company_id=7,
            rows=[self._row("ifrs-full_Assets", "BS", "1")],
            fs_div="CFS",
        )

        self.assertEqual(financials[0].disclosed_at.isoformat(), "2025-03-11")

    def test_corrected_filing_is_a_separate_row(self) -> None:
        """정정공시는 접수번호가 달라 별개 행이 된다."""
        rows = [
            self._row("ifrs-full_Assets", "BS", "100"),
            self._row("ifrs-full_Assets", "BS", "200", rcept_no="20250520001111"),
        ]

        financials = build_dart_financials(company_id=7, rows=rows, fs_div="CFS")

        self.assertEqual(len(financials), 2)
        self.assertEqual({item.total_assets for item in financials}, {100, 200})

    def test_missing_amount_is_skipped(self) -> None:
        """'-'나 빈 금액은 0이 아니라 없음이다."""
        financials = build_dart_financials(
            company_id=7,
            rows=[self._row("ifrs-full_Assets", "BS", "-")],
            fs_div="CFS",
        )

        self.assertEqual(financials, [])


if __name__ == "__main__":
    unittest.main()
