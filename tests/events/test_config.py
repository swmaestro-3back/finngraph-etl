"""EventSettings 기본값과 env alias."""

from __future__ import annotations

from pipelines.events.config import EventSettings


def test_defaults_without_env_file(monkeypatch):
    for key in (
        "NEWS_EVENT_MIN_SIZE",
        "NEWS_EVENT_SCAN_DAYS",
        "NEWS_EVENT_MAX_ITEMS_PER_RUN",
        "NEWS_EVENT_LLM_MAX_CONCURRENCY",
        "NEWS_EVENT_LEAD_CHARS",
        "NEWS_EVENT_TITLE_MAX_CHARS",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = EventSettings(_env_file=None)

    assert settings.min_size == 5
    assert settings.scan_days == 15
    assert settings.max_items_per_run == 50
    assert settings.llm_max_concurrency == 4
    assert settings.lead_chars == 600
    assert settings.title_max_chars == 30


def test_env_alias_overrides(monkeypatch):
    monkeypatch.setenv("NEWS_EVENT_MIN_SIZE", "3")
    monkeypatch.setenv("NEWS_EVENT_MAX_ITEMS_PER_RUN", "7")

    settings = EventSettings(_env_file=None)

    assert settings.min_size == 3
    assert settings.max_items_per_run == 7
