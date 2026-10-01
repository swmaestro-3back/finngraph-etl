"""news 설정 기본값 테스트. env 캐스팅은 conftest 가 주입한다."""

from __future__ import annotations


def test_search_defaults():
    from pipelines.news.config import NewsSettings

    settings = NewsSettings(_env_file=None)

    assert settings.search_sort == "date"
    assert settings.search_query_templates == ["특징주,{name}", "{name}"]
    assert settings.search_max_pages == 3
    assert settings.search_lookback_days == 180
    assert settings.search_interval_hours == 2
    assert settings.news_llm_batch_size == 10


def test_search_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_SEARCH_QUERY_TEMPLATES", '["{name}"]')
    monkeypatch.setenv("NEWS_SEARCH_MAX_PAGES", "5")
    monkeypatch.setenv("NEWS_SEARCH_LOOKBACK_DAYS", "30")
    monkeypatch.setenv("NEWS_SEARCH_INTERVAL_HOURS", "1")

    settings = NewsSettings(_env_file=None)

    assert settings.search_query_templates == ["{name}"]
    assert settings.search_max_pages == 5
    assert settings.search_lookback_days == 30
    assert settings.search_interval_hours == 1


def test_issue_timeline_defaults():
    from pipelines.news.config import NewsSettings

    settings = NewsSettings(_env_file=None)

    assert settings.cluster_summary_max_chars == 150
    # 백필 전에 스케줄 연결이 돌지 않도록 기본은 꺼져 있다
    assert settings.issue_link_enabled is False
    assert settings.issue_link_threshold == 0.6
    assert settings.issue_link_no_company_threshold == 0.75
    assert settings.issue_link_lookback_days == 90
    assert settings.issue_link_max_per_run == 200
    assert settings.issue_embedding_model == "amazon.titan-embed-text-v2:0"
