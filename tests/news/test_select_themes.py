from __future__ import annotations

from datetime import date

import pytest
import redis

from pipelines.news.jobs import select_themes as job
from pipelines.news.repositories.hot_themes import HotThemes, HotThemesUnavailableError


def _raise(exc: Exception):
    def raiser(*args, **kwargs):
        raise exc

    return raiser


def test_explicit_theme_ids_bypass_everything(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", _raise(AssertionError("Redis 조회됨")))
    monkeypatch.setattr(job, "fetch_latest_trade_date", _raise(AssertionError("DB 조회됨")))

    assert job.run([12, "34"]) == [12, 34]


def test_uses_backend_hot_themes_when_trade_date_matches(monkeypatch):
    d = date(2026, 9, 11)
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(d, [7, 3, 9]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: d)

    assert job.run(None) == [7, 3, 9]
    assert job.run([]) == [7, 3, 9]


def test_fails_when_hot_themes_stale(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(date(2026, 9, 10), [7, 3, 9]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: date(2026, 9, 11))

    with pytest.raises(HotThemesUnavailableError, match="기준일 불일치"):
        job.run(None)


def test_fails_when_hot_themes_trade_date_null(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(None, [7]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: date(2026, 9, 11))

    with pytest.raises(HotThemesUnavailableError, match="기준일 불일치"):
        job.run(None)


def test_propagates_hot_themes_fetch_failure(monkeypatch):
    monkeypatch.setattr(job, "fetch_latest_trade_date", _raise(AssertionError("DB 조회됨")))

    monkeypatch.setattr(job, "fetch_hot_themes", _raise(redis.ConnectionError("down")))
    with pytest.raises(redis.ConnectionError):
        job.run(None)

    monkeypatch.setattr(job, "fetch_hot_themes", _raise(HotThemesUnavailableError("키 없음")))
    with pytest.raises(HotThemesUnavailableError):
        job.run(None)
