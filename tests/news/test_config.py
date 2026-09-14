"""news 설정 기본값 테스트. env 캐스팅은 conftest 가 주입한다."""

from __future__ import annotations


def test_search_defaults():
    from pipelines.news.config import NewsSettings

    settings = NewsSettings(_env_file=None)

    assert settings.search_sort == "date"
    assert settings.search_query_template == "특징주,{name}"
    assert settings.search_max_pages == 3
    assert settings.search_lookback_days == 180
    assert settings.search_interval_hours == 4
    assert settings.news_llm_batch_size == 10


def test_search_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_SEARCH_QUERY_TEMPLATE", "{name}")
    monkeypatch.setenv("NEWS_SEARCH_MAX_PAGES", "5")
    monkeypatch.setenv("NEWS_SEARCH_LOOKBACK_DAYS", "30")
    monkeypatch.setenv("NEWS_SEARCH_INTERVAL_HOURS", "1")

    settings = NewsSettings(_env_file=None)

    assert settings.search_query_template == "{name}"
    assert settings.search_max_pages == 5
    assert settings.search_lookback_days == 30
    assert settings.search_interval_hours == 1
