"""EventSettings 기본값과 env alias."""

from __future__ import annotations

from pipelines.events.config import EventSettings


def test_defaults_without_env_file(monkeypatch):
    for key in ("NEWS_EVENT_SCAN_DAYS", "NEWS_EVENT_COMPANY_MIN_ARTICLES"):
        monkeypatch.delenv(key, raising=False)

    settings = EventSettings(_env_file=None)

    assert settings.scan_days == 15
    assert settings.company_min_articles == 2


def test_env_alias_overrides(monkeypatch):
    monkeypatch.setenv("NEWS_EVENT_SCAN_DAYS", "30")
    monkeypatch.setenv("NEWS_EVENT_COMPANY_MIN_ARTICLES", "5")

    settings = EventSettings(_env_file=None)

    assert settings.scan_days == 30
    assert settings.company_min_articles == 5
