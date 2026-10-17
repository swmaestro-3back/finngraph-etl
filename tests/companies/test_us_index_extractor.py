from __future__ import annotations

import pytest

from pipelines.companies.extractors import us_index
from pipelines.companies.extractors.us_index import (
    latest_data_folder,
    merge_index_rows,
    parse_hankyung,
    parse_wikipedia_table,
)

WIKI_NASDAQ_HTML = """
<table class="wikitable"><tbody>
<tr><th>Ticker</th><th>Company</th><th>GICS Sector</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td><td>IT</td></tr>
<tr><td>ALNY</td><td>Alnylam Pharmaceuticals</td><td>Health</td></tr>
</tbody></table>
"""

WIKI_SP500_HTML = """
<table class="wikitable"><tbody>
<tr><th>Symbol</th><th>Security</th><th>GICS Sector</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td><td>IT</td></tr>
<tr><td>JPM</td><td>JPMorgan Chase</td><td>Financials</td></tr>
<tr><td>BRK.B</td><td>Berkshire Hathaway</td><td>Financials</td></tr>
</tbody></table>
"""

HANKYUNG_HTML = """
<div class="sub-section usa-market"><table><tbody>
<tr><td><a class="stock-item" href="#">
  <div class="stock-name">애플</div><div class="symbol txt-en">AAPL</div></a></td></tr>
<tr><td><a class="stock-item" href="#">
  <div class="stock-name">JP모간체이스</div><div class="symbol txt-en">JPM</div></a></td></tr>
<tr><td><a class="stock-item" href="#">
  <div class="stock-name">버크셔 해서웨이 B</div><div class="symbol txt-en">BRK.B</div></a></td></tr>
</tbody></table></div>
"""


def test_parse_wikipedia_table_renames_columns() -> None:
    rows = parse_wikipedia_table(WIKI_NASDAQ_HTML, "Ticker", "Company", "NASDAQ", False)

    assert rows == [
        {"ticker": "AAPL", "name": "Apple Inc.", "market": "NASDAQ", "sp500": False},
        {"ticker": "ALNY", "name": "Alnylam Pharmaceuticals", "market": "NASDAQ", "sp500": False},
    ]


def test_parse_hankyung_extracts_name_and_symbol() -> None:
    assert parse_hankyung(HANKYUNG_HTML) == [
        {"kr_name": "애플", "ticker": "AAPL"},
        {"kr_name": "JP모간체이스", "ticker": "JPM"},
        {"kr_name": "버크셔 해서웨이 B", "ticker": "BRK.B"},
    ]


def test_merge_prefers_nasdaq_and_flags_sp500_and_inner_joins_hankyung() -> None:
    nasdaq = parse_wikipedia_table(WIKI_NASDAQ_HTML, "Ticker", "Company", "NASDAQ", False)
    sp500 = parse_wikipedia_table(WIKI_SP500_HTML, "Symbol", "Security", "NYSE", True)
    hankyung = parse_hankyung(HANKYUNG_HTML)

    merged = merge_index_rows(nasdaq, sp500, hankyung)

    by_ticker = {row["ticker"]: row for row in merged}
    # 두 지수에 모두 있으면 NASDAQ 유지 + sp500 true
    assert by_ticker["AAPL"] == {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "kr_name": "애플",
        "market": "NASDAQ",
        "sp500": True,
    }
    assert by_ticker["JPM"]["market"] == "NYSE" and by_ticker["JPM"]["sp500"] is True
    assert by_ticker["BRK.B"]["kr_name"] == "버크셔 해서웨이 B"
    # 한경에 없는 ALNY는 탈락
    assert "ALNY" not in by_ticker
    assert len(merged) == 3


def test_latest_data_folder_picks_newest_with_both_files(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(us_index, "DATA_ROOT", tmp_path)

    older = tmp_path / "20260101"
    older.mkdir()
    (older / "us.json").write_text("{}")
    (older / "us_overview.json").write_text("{}")

    newer_incomplete = tmp_path / "20260108"
    newer_incomplete.mkdir()
    (newer_incomplete / "us.json").write_text("{}")

    found = latest_data_folder(("us.json", "us_overview.json"))

    assert found == older


def test_latest_data_folder_ignores_non_date_dirs(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(us_index, "DATA_ROOT", tmp_path)

    foo = tmp_path / "foo"
    foo.mkdir()
    (foo / "us.json").write_text("{}")
    (foo / "us_overview.json").write_text("{}")

    dated = tmp_path / "20260101"
    dated.mkdir()
    (dated / "us.json").write_text("{}")
    (dated / "us_overview.json").write_text("{}")

    found = latest_data_folder(("us.json", "us_overview.json"))

    assert found == dated


def test_latest_data_folder_raises_when_nothing_qualifies(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(us_index, "DATA_ROOT", tmp_path)

    incomplete = tmp_path / "20260101"
    incomplete.mkdir()
    (incomplete / "us.json").write_text("{}")

    with pytest.raises(FileNotFoundError):
        latest_data_folder(("us.json", "us_overview.json"))
