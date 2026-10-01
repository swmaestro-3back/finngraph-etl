from __future__ import annotations

from datetime import date

import pytest
import redis

from pipelines.news.jobs import select_themes as job
from pipelines.news.jobs.select_themes import ThemeSelection
from pipelines.news.repositories.hot_themes import HotThemes, HotThemesUnavailableError
from pipelines.news.repositories.trade_dates import TradeDates

SETTLED = date(2026, 9, 10)
INTRADAY = date(2026, 9, 11)


def _raise(exc: Exception):
    def raiser(*args, **kwargs):
        raise exc

    return raiser


def _patch(monkeypatch, hot, dates):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: hot)
    monkeypatch.setattr(job, "fetch_trade_dates", lambda as_of: dates)


def test_explicit_theme_ids_bypass_everything(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", _raise(AssertionError("Redis 조회됨")))
    monkeypatch.setattr(job, "fetch_trade_dates", _raise(AssertionError("DB 조회됨")))

    assert job.run([12, "34"]) == ThemeSelection([12, 34])


def test_uses_backend_hot_themes_when_trade_date_is_settled(monkeypatch):
    _patch(monkeypatch, HotThemes(SETTLED, [7, 3, 9]), TradeDates(SETTLED, SETTLED))

    assert job.run(None) == ThemeSelection([7, 3, 9])
    assert job.run([]) == ThemeSelection([7, 3, 9])


def test_uses_intraday_hot_themes_before_valuations_settle(monkeypatch):
    _patch(monkeypatch, HotThemes(INTRADAY, [7, 3, 9]), TradeDates(SETTLED, INTRADAY))

    assert job.run(None) == ThemeSelection([7, 3, 9])


def test_passes_backend_hot_theme_tickers_as_search_targets(monkeypatch):
    hot = HotThemes(INTRADAY, [7, 3], ["005490", "010140"])
    _patch(monkeypatch, hot, TradeDates(SETTLED, INTRADAY))

    assert job.run(None) == ThemeSelection([7, 3], ["005490", "010140"])


def test_keeps_settled_hot_themes_until_intraday_publish_arrives(monkeypatch):
    _patch(monkeypatch, HotThemes(SETTLED, [7, 3, 9]), TradeDates(SETTLED, INTRADAY))

    assert job.run(None) == ThemeSelection([7, 3, 9])


@pytest.mark.parametrize(
    ("trade_date", "dates"),
    [
        (date(2026, 9, 9), TradeDates(SETTLED, INTRADAY)),  # 마감일보다 오래됨
        (date(2026, 9, 14), TradeDates(SETTLED, INTRADAY)),  # 최신 일봉보다 새로움
        (None, TradeDates(SETTLED, SETTLED)),  # 페이로드에 기준일 없음
        (SETTLED, TradeDates(None, None)),  # DB 에 일봉 없음
    ],
)
def test_fails_when_hot_themes_trade_date_out_of_range(monkeypatch, trade_date, dates):
    _patch(monkeypatch, HotThemes(trade_date, [7, 3, 9]), dates)

    with pytest.raises(HotThemesUnavailableError, match="DB 범위 밖"):
        job.run(None)


def test_propagates_hot_themes_fetch_failure(monkeypatch):
    monkeypatch.setattr(job, "fetch_trade_dates", _raise(AssertionError("DB 조회됨")))

    monkeypatch.setattr(job, "fetch_hot_themes", _raise(redis.ConnectionError("down")))
    with pytest.raises(redis.ConnectionError):
        job.run(None)

    monkeypatch.setattr(job, "fetch_hot_themes", _raise(HotThemesUnavailableError("키 없음")))
    with pytest.raises(HotThemesUnavailableError):
        job.run(None)
