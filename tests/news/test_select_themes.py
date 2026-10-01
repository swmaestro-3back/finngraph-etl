from __future__ import annotations

from datetime import date

from pipelines.news.jobs import select_themes as job
from pipelines.news.jobs.select_themes import ThemeSelection
from pipelines.news.repositories.hot_themes import HotThemes
from pipelines.news.repositories.theme_changes import ThemeChange, TradeDates

SETTLED = date(2026, 9, 10)
INTRADAY = date(2026, 9, 11)


def _patch(monkeypatch, hot, dates):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: hot)
    monkeypatch.setattr(job, "fetch_trade_dates", lambda as_of: dates)
    monkeypatch.setattr(
        job,
        "fetch_theme_changes",
        lambda as_of: [ThemeChange(1, "a", SETTLED, 5.0, 5)],
    )


def test_explicit_theme_ids_bypass_everything(monkeypatch):
    monkeypatch.setattr(
        job, "fetch_hot_themes", lambda: (_ for _ in ()).throw(AssertionError("Redis 조회됨"))
    )
    monkeypatch.setattr(
        job, "fetch_theme_changes", lambda as_of: (_ for _ in ()).throw(AssertionError("DB 조회됨"))
    )

    assert job.run([12, "34"]) == ThemeSelection([12, 34])


def test_uses_backend_hot_themes_when_trade_date_is_settled(monkeypatch):
    _patch(monkeypatch, HotThemes(SETTLED, [7, 3, 9]), TradeDates(SETTLED, SETTLED))
    monkeypatch.setattr(
        job, "fetch_theme_changes", lambda as_of: (_ for _ in ()).throw(AssertionError("폴백됨"))
    )

    assert job.run(None) == ThemeSelection([7, 3, 9])


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


def test_falls_back_when_hot_themes_older_than_settled_date(monkeypatch):
    _patch(monkeypatch, HotThemes(date(2026, 9, 9), [7, 3, 9]), TradeDates(SETTLED, INTRADAY))

    assert job.run(None) == ThemeSelection([1])


def test_falls_back_when_hot_themes_newer_than_latest_candles(monkeypatch):
    _patch(monkeypatch, HotThemes(date(2026, 9, 14), [7, 3, 9]), TradeDates(SETTLED, INTRADAY))

    assert job.run(None) == ThemeSelection([1])


def test_falls_back_when_hot_themes_trade_date_null(monkeypatch):
    _patch(monkeypatch, HotThemes(None, [7]), TradeDates(SETTLED, SETTLED))

    assert job.run(None) == ThemeSelection([1])


def test_falls_back_when_db_has_no_candles(monkeypatch):
    _patch(monkeypatch, HotThemes(SETTLED, [7]), TradeDates(None, None))

    assert job.run(None) == ThemeSelection([1])


def test_selects_momentum_themes_from_latest_candles(monkeypatch):
    from pipelines.news import config

    monkeypatch.setenv("NEWS_THEME_COUNT", "2")
    config.get_news_settings.cache_clear()
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: None)
    seen: list[date] = []

    def fake_fetch(as_of):
        seen.append(as_of)
        d = date(2026, 9, 11)
        return [
            ThemeChange(1, "a", d, 5.0, 5),
            ThemeChange(2, "b", d, -3.0, 5),
            ThemeChange(3, "c", d, 1.0, 5),
        ]

    monkeypatch.setattr(job, "fetch_theme_changes", fake_fetch)
    try:
        assert job.run([]) == ThemeSelection([1, 2])
        assert job.run(None) == ThemeSelection([1, 2])
    finally:
        config.get_news_settings.cache_clear()

    assert len(seen) == 2 and all(isinstance(d, date) for d in seen)
