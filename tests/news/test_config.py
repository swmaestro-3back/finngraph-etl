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
    # 관련성 묶음 호출 설정은 jev 전환으로 없다
    assert not hasattr(settings, "news_llm_batch_size")


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


CLUSTER_ENV = (
    "NEWS_CLUSTER_THRESHOLD",
    "NEWS_CLUSTER_TEXT_WEIGHTS",
    "NEWS_CLUSTER_TIME_WEIGHTS",
    "NEWS_CLUSTER_TIME_SIGMA_HOURS",
    "NEWS_CLUSTER_CANDIDATE_DAYS",
    "NEWS_CLUSTER_MAX_SPAN_DAYS",
    "NEWS_CLUSTER_IDF_DAYS",
    "NEWS_CLUSTER_PROMOTE_SIZE",
    "NEWS_CLUSTER_PROMOTE_RETRY_DAYS",
    "NEWS_CLUSTER_REPRESENTATIVE_MIN_CHARS",
    "NEWS_CLUSTER_KEYWORD_COUNT",
    "NEWS_CLUSTER_TITLE_MAX_CHARS",
    "NEWS_CLUSTER_TITLE_HEADLINES",
)


def test_cluster_defaults(monkeypatch):
    from pipelines.news.config import NewsSettings

    for key in CLUSTER_ENV:
        monkeypatch.delenv(key, raising=False)

    s = NewsSettings(_env_file=None)

    assert s.cluster_threshold == 1.0
    assert s.cluster_text_weights == (1.0, 1.0, 1.0, 1.0)
    assert s.cluster_time_weights == (0.1, 0.1, 0.1)
    assert (s.cluster_time_sigma_hours, s.cluster_candidate_days, s.cluster_max_span_days) == (
        72.0,
        10,
        10,
    )
    assert (s.cluster_idf_days, s.cluster_promote_size, s.cluster_promote_retry_days) == (90, 3, 3)
    assert (s.cluster_representative_min_chars, s.cluster_keyword_count) == (200, 6)
    assert (s.cluster_title_max_chars, s.cluster_title_headlines) == (25, 10)
    # 단일 벡터·고정 시간 창 시절 설정은 없다
    for removed in (
        "cluster_description_weight",
        "cluster_lead_chars",
        "cluster_window_days",
        "cluster_backward_days",
    ):
        assert not hasattr(s, removed)


def test_cluster_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_CLUSTER_PROMOTE_SIZE", "5")
    monkeypatch.setenv("NEWS_CLUSTER_THRESHOLD", "1.5")
    monkeypatch.setenv("NEWS_CLUSTER_TEXT_WEIGHTS", "0.5, 1, 2,0.25")
    monkeypatch.setenv("NEWS_CLUSTER_TIME_WEIGHTS", "0,0,1")
    monkeypatch.setenv("NEWS_CLUSTER_MAX_SPAN_DAYS", "14")

    s = NewsSettings(_env_file=None)

    assert s.cluster_promote_size == 5
    assert s.cluster_threshold == 1.5
    assert s.cluster_text_weights == (0.5, 1.0, 2.0, 0.25)
    assert s.cluster_time_weights == (0.0, 0.0, 1.0)
    assert s.cluster_max_span_days == 14


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("NEWS_CLUSTER_TEXT_WEIGHTS", "1,1,1"),
        ("NEWS_CLUSTER_TEXT_WEIGHTS", "1,1,1,1,1"),
        ("NEWS_CLUSTER_TIME_WEIGHTS", "0.1,0.1"),
        ("NEWS_CLUSTER_TIME_WEIGHTS", "a,b,c"),
    ],
)
def test_cluster_weight_length_is_validated(monkeypatch, name, value):
    """가중치 개수가 피처 수와 다르면 task 가 시작 시점에 실패해야 한다."""
    from pydantic import ValidationError

    from pipelines.news.config import NewsSettings

    monkeypatch.setenv(name, value)
    with pytest.raises(ValidationError):
        NewsSettings(_env_file=None)


def test_body_candidate_max_default(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.delenv("NEWS_BODY_CANDIDATE_MAX", raising=False)

    assert NewsSettings(_env_file=None).news_body_candidate_max == 15


JEV_THRESHOLD_ENV = (
    "NEWS_RELEVANCE_COMPANY_MIN",
    "NEWS_RELEVANCE_GATE_MIN",
    "NEWS_ENTITY_COMPANY_MIN",
    "NEWS_ENTITY_PARTY_MIN",
)


def test_jev_threshold_defaults(monkeypatch):
    from pipelines.news.config import NewsSettings

    for key in JEV_THRESHOLD_ENV:
        monkeypatch.delenv(key, raising=False)

    s = NewsSettings(_env_file=None)

    assert (s.relevance_company_min, s.relevance_gate_min) == (0.7, 0.55)
    assert (s.entity_company_min, s.entity_party_min) == (0.5, 0.5)


def test_jev_thresholds_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

    monkeypatch.setenv("NEWS_RELEVANCE_COMPANY_MIN", "0.8")
    monkeypatch.setenv("NEWS_RELEVANCE_GATE_MIN", "0.6")
    monkeypatch.setenv("NEWS_ENTITY_COMPANY_MIN", "0.55")
    monkeypatch.setenv("NEWS_ENTITY_PARTY_MIN", "0.7")

    s = NewsSettings(_env_file=None)

    assert (s.relevance_company_min, s.relevance_gate_min) == (0.8, 0.6)
    assert (s.entity_company_min, s.entity_party_min) == (0.55, 0.7)


@pytest.mark.parametrize("name", JEV_THRESHOLD_ENV)
def test_jev_threshold_must_be_a_probability(monkeypatch, name):
    from pydantic import ValidationError

    from pipelines.news.config import NewsSettings

    monkeypatch.setenv(name, "1.5")
    with pytest.raises(ValidationError):
        NewsSettings(_env_file=None)


def test_env_example_lists_jev_settings():
    from pathlib import Path

    env_example = (Path(__file__).resolve().parents[2] / ".env.example").read_text()

    for key in ("JEV_API_KEY", "JEV_MODEL", "JEV_REQUEST_TIMEOUT", *JEV_THRESHOLD_ENV):
        assert f"\n{key}=" in env_example, key
    # jev 가 맡은 역할의 Bedrock 모델과 묶음 크기는 없다
    for removed in ("BEDROCK_RELEVANCE_MODEL", "BEDROCK_ENTITY_MODEL", "NEWS_LLM_BATCH_SIZE"):
        assert f"\n{removed}=" not in env_example, removed


ISSUE_LINK_ENV = (
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

    assert settings.issue_link_threshold == 0.45
    assert settings.issue_link_no_company_threshold == 0.75
    assert settings.issue_link_lookback_days == 90
    assert settings.issue_link_max_per_run == 200
    assert settings.issue_same_event_max_gap_hours == 24
    assert settings.issue_same_event_score == 0.75
    # 코사인만으로 같은 사건이라고 보는 간격 상한(7일). 클러스터 수명 NEWS_CLUSTER_MAX_SPAN_DAYS(10일) 안이다.
    assert settings.issue_same_event_score_max_gap_hours == 168
    assert settings.issue_relink_window_hours == 72
    assert settings.issue_embedding_model == "amazon.titan-embed-text-v2:0"
    # 판정은 LLM 투표가 기본이고, 코사인 규칙은 되돌리기용이다.
    assert settings.issue_link_method == "vote"
    assert settings.issue_link_llm_max_calls_per_run == 1500
    assert settings.issue_link_llm_max_calls_per_issue == 150


def test_issue_link_settings_read_env(monkeypatch):
    from pipelines.news.config import NewsSettings

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
