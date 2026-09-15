from __future__ import annotations

from datetime import date

from pipelines.news.jobs import select_themes as job
from pipelines.news.repositories.hot_themes import HotThemes
from pipelines.news.repositories.theme_changes import ThemeChange


def test_explicit_theme_ids_bypass_everything(monkeypatch):
    monkeypatch.setattr(
        job, "fetch_hot_themes", lambda: (_ for _ in ()).throw(AssertionError("Redis 조회됨"))
    )
    monkeypatch.setattr(
        job, "fetch_theme_changes", lambda as_of: (_ for _ in ()).throw(AssertionError("DB 조회됨"))
    )

    assert job.run([12, "34"]) == [12, 34]


def test_uses_backend_hot_themes_when_trade_date_matches(monkeypatch):
    d = date(2026, 9, 11)
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(d, [7, 3, 9]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: d)
    monkeypatch.setattr(
        job, "fetch_theme_changes", lambda as_of: (_ for _ in ()).throw(AssertionError("폴백됨"))
    )

    assert job.run(None) == [7, 3, 9]


def test_falls_back_when_hot_themes_stale(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(date(2026, 9, 10), [7, 3, 9]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: date(2026, 9, 11))
    monkeypatch.setattr(
        job,
        "fetch_theme_changes",
        lambda as_of: [ThemeChange(1, "a", date(2026, 9, 11), 5.0, 5)],
    )

    assert job.run(None) == [1]


def test_falls_back_when_hot_themes_trade_date_null(monkeypatch):
    monkeypatch.setattr(job, "fetch_hot_themes", lambda: HotThemes(None, [7]))
    monkeypatch.setattr(job, "fetch_latest_trade_date", lambda as_of: date(2026, 9, 11))
    monkeypatch.setattr(
        job,
        "fetch_theme_changes",
        lambda as_of: [ThemeChange(1, "a", date(2026, 9, 11), 5.0, 5)],
    )

    assert job.run(None) == [1]


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
        assert job.run([]) == [1, 2]
        assert job.run(None) == [1, 2]
    finally:
        config.get_news_settings.cache_clear()

    assert len(seen) == 2 and all(isinstance(d, date) for d in seen)
