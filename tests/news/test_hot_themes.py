from __future__ import annotations

import json
from datetime import date

import redis

from pipelines.news.repositories import hot_themes as repo


def payload(**overrides):
    base = {
        "tradeDate": "2026-07-31",
        "generatedAt": "2026-09-16T15:40:00+09:00",
        "count": 2,
        "themes": [
            {
                "id": 12,
                "name": "철강",
                "change": 3.21,
                "stocks": [{"ticker": "005490", "name": "POSCO홀딩스"}],
            },
            {"id": 34, "name": "조선", "change": -2.1, "stocks": []},
        ],
    }
    base.update(overrides)
    return json.dumps(base)


def test_parse_returns_ids_in_payload_order():
    parsed = repo.parse_hot_themes(payload())

    assert parsed is not None
    assert parsed.trade_date == date(2026, 7, 31)
    assert parsed.theme_ids == [12, 34]
    assert parsed.tickers == ["005490"]


def test_parse_collects_tickers_once_in_payload_order():
    parsed = repo.parse_hot_themes(
        payload(
            themes=[
                {"id": 1, "stocks": [{"ticker": "000660"}, {"ticker": "005930"}]},
                {"id": 2, "stocks": [{"ticker": "005930"}, {"ticker": "042700"}]},
                {"id": 3},
            ]
        )
    )

    assert parsed is not None
    assert parsed.theme_ids == [1, 2, 3]
    assert parsed.tickers == ["000660", "005930", "042700"]


def test_parse_rejects_contract_violations():
    assert repo.parse_hot_themes("not-json") is None
    assert repo.parse_hot_themes(json.dumps({"themes": "oops"})) is None
    assert repo.parse_hot_themes(json.dumps({"tradeDate": "2026-07-31"})) is None
    assert repo.parse_hot_themes(payload(themes=[])) is None
    assert repo.parse_hot_themes(payload(themes=[{"name": "id 없음"}])) is None
    assert repo.parse_hot_themes(payload(themes=[{"id": 1, "stocks": "oops"}])) is None
    assert repo.parse_hot_themes(payload(themes=[{"id": 1, "stocks": [{"name": "x"}]}])) is None


def test_parse_allows_null_trade_date():
    parsed = repo.parse_hot_themes(payload(tradeDate=None))

    assert parsed is not None
    assert parsed.trade_date is None
    assert parsed.theme_ids == [12, 34]


class FakeRedis:
    def __init__(self, raw):
        self.raw = raw

    def get(self, key):
        assert key == repo.HOT_THEMES_KEY
        return self.raw

    def close(self):
        pass


def test_fetch_returns_parsed_payload(monkeypatch):
    monkeypatch.setattr(repo.redis.Redis, "from_url", lambda *a, **k: FakeRedis(payload()))

    fetched = repo.fetch_hot_themes()

    assert fetched is not None and fetched.theme_ids == [12, 34]


def test_fetch_falls_back_to_none_when_key_missing(monkeypatch):
    monkeypatch.setattr(repo.redis.Redis, "from_url", lambda *a, **k: FakeRedis(None))

    assert repo.fetch_hot_themes() is None


def test_fetch_falls_back_to_none_on_redis_error(monkeypatch):
    def raise_error(*a, **k):
        raise redis.ConnectionError("down")

    monkeypatch.setattr(repo.redis.Redis, "from_url", raise_error)

    assert repo.fetch_hot_themes() is None
