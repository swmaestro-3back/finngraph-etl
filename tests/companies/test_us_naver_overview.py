from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from pipelines.companies.extractors import naver_overview

ROWS = [
    {"ticker": "AAPL", "name": "Apple Inc.", "kr_name": "애플", "market": "NASDAQ", "sp500": True},
    {"ticker": "JPM", "name": "JPMorgan", "kr_name": "JP모건", "market": "NYSE", "sp500": True},
    {"ticker": "ZZZZ", "name": "Ghost", "kr_name": "유령", "market": "NYSE", "sp500": False},
]

# 네이버가 실제로 응답하는 코드만. NYSE 종목은 접미 없음 → .K → .O 순으로 훑다 .K에서 걸린다.
LIVE = {
    "AAPL.O": {"companyName": "애플"},
    "JPM.K": {"companyName": "JP모건"},
}


@pytest.fixture
def fake_fetch(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    called: list[str] = []

    async def _fetch_one(code: str) -> dict | None:
        called.append(code)
        return LIVE.get(code)

    monkeypatch.setattr(naver_overview, "_fetch_one", _fetch_one)
    return called


def test_fetch_overviews_resolves_first_responding_candidate(fake_fetch: list[str]) -> None:
    result = asyncio.run(naver_overview.fetch_overviews(ROWS))

    # 티커 안의 순서는 후보 순서 그대로, 티커 사이 순서는 동시 실행이라 보장되지 않는다.
    assert sorted(fake_fetch) == sorted(["AAPL.O", "JPM", "JPM.K", "ZZZZ", "ZZZZ.K", "ZZZZ.O"])
    assert [c for c in fake_fetch if c.startswith("JPM")] == ["JPM", "JPM.K"]
    assert [c for c in fake_fetch if c.startswith("ZZZZ")] == ["ZZZZ", "ZZZZ.K", "ZZZZ.O"]
    assert result == {
        "AAPL": {"reuters_code": "AAPL.O", "overview": {"companyName": "애플"}},
        "JPM": {"reuters_code": "JPM.K", "overview": {"companyName": "JP모건"}},
    }


def test_crawl_us_overview_writes_next_to_index(tmp_path: Path, fake_fetch: list[str]) -> None:
    index_path = tmp_path / "us.json"
    index_path.write_text(json.dumps(ROWS), encoding="utf-8")

    output = asyncio.run(naver_overview.crawl_us_overview(index_path))

    assert output == tmp_path / "us_overview.json"
    saved = json.loads(output.read_text(encoding="utf-8"))
    assert set(saved) == {"AAPL", "JPM"}
    assert saved["JPM"]["reuters_code"] == "JPM.K"


def test_fetch_overviews_handles_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _get_json(code: str) -> dict | None:
        if code.startswith("ZZZZ"):
            raise naver_overview._RetryableHTTPError("boom")
        return LIVE.get(code)

    monkeypatch.setattr(naver_overview, "_get_json", _get_json)

    result = asyncio.run(naver_overview.fetch_overviews(ROWS))

    assert set(result) == {"AAPL", "JPM"}


def test_fetch_overviews_skips_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """재시도 대상이 아닌 예외(JSON 파싱 실패 등)도 그 코드만 버리고 배치를 잇는다."""

    async def _get_json(code: str) -> dict | None:
        if code.startswith("ZZZZ"):
            raise ValueError("not json")
        return LIVE.get(code)

    monkeypatch.setattr(naver_overview, "_get_json", _get_json)

    result = asyncio.run(naver_overview.fetch_overviews(ROWS))

    assert set(result) == {"AAPL", "JPM"}
    assert result["JPM"]["reuters_code"] == "JPM.K"
