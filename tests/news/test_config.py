"""news 설정 기본값 테스트. env 캐스팅은 conftest 가 주입한다."""

from __future__ import annotations


def test_search_defaults():
    from pipelines.news.config import NewsSettings

    settings = NewsSettings(_env_file=None)

    assert settings.search_sort == "date"
    assert settings.search_query_templates == [
        "{name},공급",
        "{name},계약",
        "{name},수혜",
        "{name},호재",
        "{name},악재",
        "{name},특징주",
    ]
    assert settings.search_max_pages == 5
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


def test_cluster_promotion_defaults(monkeypatch):
    from pipelines.news.config import NewsSettings

    for key in (
        "NEWS_CLUSTER_BACKWARD_DAYS",
        "NEWS_CLUSTER_IDF_DAYS",
        "NEWS_CLUSTER_PROMOTE_SIZE",
        "NEWS_CLUSTER_PROMOTE_RETRY_DAYS",
        "NEWS_CLUSTER_REPRESENTATIVE_MIN_CHARS",
        "NEWS_CLUSTER_LEAD_CHARS",
    ):
        monkeypatch.delenv(key, raising=False)

    settings = NewsSettings(_env_file=None)

    assert settings.cluster_backward_days == 1
    assert settings.cluster_idf_days == 90
    assert settings.cluster_promote_size == 3
    assert settings.cluster_promote_retry_days == 3
    assert settings.cluster_representative_min_chars == 200
    assert settings.cluster_lead_chars == 200


def test_cluster_promotion_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_CLUSTER_PROMOTE_SIZE", "5")
    monkeypatch.setenv("NEWS_CLUSTER_BACKWARD_DAYS", "2")

    settings = NewsSettings(_env_file=None)

    assert settings.cluster_promote_size == 5
    assert settings.cluster_backward_days == 2


def test_body_candidate_max_default(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.delenv("NEWS_BODY_CANDIDATE_MAX", raising=False)

    assert NewsSettings(_env_file=None).news_body_candidate_max == 15
