"""select_themes job 단위 테스트. 등락률 조회는 갈아끼운다."""

from __future__ import annotations

from datetime import date

from pipelines.news.jobs import select_themes as job
from pipelines.news.repositories.theme_changes import ThemeChange


def test_explicit_theme_ids_bypass_momentum(monkeypatch):
    monkeypatch.setattr(
        job, "fetch_theme_changes", lambda as_of: (_ for _ in ()).throw(AssertionError("DB 조회됨"))
    )

    assert job.run([12, "34"]) == [12, 34]


def test_selects_momentum_themes_from_latest_candles(monkeypatch):
    from pipelines.news import config

    monkeypatch.setenv("NEWS_THEME_COUNT", "2")
    config.get_news_settings.cache_clear()
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
