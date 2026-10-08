"""news 설정 기본값 테스트. env 캐스팅은 conftest 가 주입한다."""

from __future__ import annotations

import pytest


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


ISSUE_LINK_ENV = (
    "NEWS_ISSUE_LINK_ENABLED",
    "NEWS_ISSUE_LINK_THRESHOLD",
    "NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD",
    "NEWS_ISSUE_LINK_LOOKBACK_DAYS",
    "NEWS_ISSUE_LINK_MAX_PER_RUN",
    "NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS",
    "NEWS_ISSUE_SAME_EVENT_SCORE",
    "NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS",
    "NEWS_ISSUE_RELINK_WINDOW_HOURS",
    "NEWS_ISSUE_EMBEDDING_MODEL",
    "NEWS_ISSUE_LINK_METHOD",
    "NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN",
    "NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE",
)


def test_issue_link_defaults(monkeypatch):
    from pipelines.news.config import NewsSettings

    for key in ISSUE_LINK_ENV:
        monkeypatch.delenv(key, raising=False)

    settings = NewsSettings(_env_file=None)

    # NEWS_ISSUE_LINK_ENABLED 는 꺼진 채로 배포하고, 연결 백필이 끝난 뒤 켠다.
    assert settings.issue_link_enabled is False
    assert settings.issue_link_threshold == 0.45
    assert settings.issue_link_no_company_threshold == 0.75
    assert settings.issue_link_lookback_days == 90
    assert settings.issue_link_max_per_run == 200
    assert settings.issue_same_event_max_gap_hours == 24
    assert settings.issue_same_event_score == 0.75
    # 코사인만으로 같은 사건이라고 보는 간격 상한은 클러스터 기간(NEWS_CLUSTER_WINDOW_DAYS, 7일)과 같다.
    assert settings.issue_same_event_score_max_gap_hours == 168
    assert settings.issue_relink_window_hours == 72
    assert settings.issue_embedding_model == "amazon.titan-embed-text-v2:0"
    # 판정은 LLM 투표가 기본이고, 코사인 규칙은 되돌리기용이다.
    assert settings.issue_link_method == "vote"
    assert settings.issue_link_llm_max_calls_per_run == 1500
    assert settings.issue_link_llm_max_calls_per_issue == 150


def test_issue_link_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_ISSUE_LINK_ENABLED", "true")
    monkeypatch.setenv("NEWS_ISSUE_LINK_THRESHOLD", "0.5")
    monkeypatch.setenv("NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD", "0.8")
    monkeypatch.setenv("NEWS_ISSUE_LINK_LOOKBACK_DAYS", "30")
    monkeypatch.setenv("NEWS_ISSUE_LINK_MAX_PER_RUN", "50")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS", "12")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_SCORE", "0.9")
    monkeypatch.setenv("NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS", "72")
    monkeypatch.setenv("NEWS_ISSUE_RELINK_WINDOW_HOURS", "0")
    monkeypatch.setenv("NEWS_ISSUE_EMBEDDING_MODEL", "titan-test")
    monkeypatch.setenv("NEWS_ISSUE_LINK_METHOD", "cosine")
    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN", "10")
    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE", "5")

    settings = NewsSettings(_env_file=None)

    assert settings.issue_link_enabled is True
    assert settings.issue_link_threshold == 0.5
    assert settings.issue_link_no_company_threshold == 0.8
    assert settings.issue_link_lookback_days == 30
    assert settings.issue_link_max_per_run == 50
    assert settings.issue_same_event_max_gap_hours == 12
    assert settings.issue_same_event_score == 0.9
    assert settings.issue_same_event_score_max_gap_hours == 72
    assert settings.issue_relink_window_hours == 0
    assert settings.issue_embedding_model == "titan-test"
    assert settings.issue_link_method == "cosine"
    assert settings.issue_link_llm_max_calls_per_run == 10
    assert settings.issue_link_llm_max_calls_per_issue == 5


def test_issue_link_method_rejects_unknown(monkeypatch):
    from pydantic import ValidationError

    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_ISSUE_LINK_METHOD", "llm")
    with pytest.raises(ValidationError):
        NewsSettings(_env_file=None)


def test_env_example_lists_issue_link_settings():
    from pathlib import Path

    env_example = (Path(__file__).resolve().parents[2] / ".env.example").read_text()

    for key in ISSUE_LINK_ENV:
        assert f"\n{key}=" in env_example, key
