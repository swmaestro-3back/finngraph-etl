"""company_filter 단위 테스트 (DB 불필요)."""

from __future__ import annotations

from pipelines.themes.models import Company, Theme
from pipelines.themes.transformers.company_filter import filter_companies


def _theme(*companies: tuple[str, str]) -> Theme:
    return Theme(
        name="반도체",
        source="naver",
        companies=[Company(name=name, ticker=ticker) for name, ticker in companies],
    )


def test_filter_companies_keeps_only_matching_ticker_and_name():
    theme = _theme(("삼성전자", "005930"), ("SK하이닉스", "000660"), ("없는회사", "999999"))
    ticker_to_name = {"005930": "삼성전자", "000660": "에스케이하이닉스"}

    [filtered] = filter_companies([theme], ticker_to_name)

    # 000660 은 종목명 불일치, 999999 는 stocks 에 없음 → 삼성전자만 남는다
    assert [c.ticker for c in filtered.companies] == ["005930"]


def test_filter_companies_with_empty_mapping_drops_everything():
    theme = _theme(("삼성전자", "005930"))

    [filtered] = filter_companies([theme], {})

    assert filtered.companies == []
