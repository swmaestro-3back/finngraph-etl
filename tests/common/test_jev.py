"""jev 공용 헬퍼 — 접속 설정과 청크 요약 로그에 붙는 호출 지연 문자열."""

from __future__ import annotations

import pytest

from pipelines.common import config
from pipelines.common.clients.jev import async_client, latency_summary
from pipelines.common.config import Settings


def test_summary_reports_count_p50_p95_and_max():
    latencies = [float(i) for i in range(1, 101)]  # 1..100ms

    assert latency_summary(latencies) == "jev 100건 p50 50ms p95 95ms 최대 100ms"


def test_summary_uses_seconds_above_one_second():
    assert latency_summary([180.0, 200.0, 1400.0]) == "jev 3건 p50 200ms p95 1.4s 최대 1.4s"


def test_summary_of_nothing():
    assert latency_summary([]) == "jev 0건"


def test_jev_settings_defaults(monkeypatch):
    for key in ("JEV_API_KEY", "JEV_MODEL", "JEV_REQUEST_TIMEOUT"):
        monkeypatch.delenv(key, raising=False)

    settings = Settings(_env_file=None)

    assert (settings.jev_api_key, settings.jev_model, settings.jev_request_timeout) == (
        "",
        "jev-latest",
        60,
    )


def test_jev_settings_read_env(monkeypatch):
    monkeypatch.setenv("JEV_API_KEY", "k")
    monkeypatch.setenv("JEV_MODEL", "jev-1.14.0")
    monkeypatch.setenv("JEV_REQUEST_TIMEOUT", "15")

    settings = Settings(_env_file=None)

    assert (settings.jev_api_key, settings.jev_model, settings.jev_request_timeout) == (
        "k",
        "jev-1.14.0",
        15,
    )


def test_async_client_refuses_to_start_without_api_key(monkeypatch):
    # 환경변수가 .env 보다 우선이다 — 빈 키는 task 시작 시점에 바로 드러나야 한다
    monkeypatch.setenv("JEV_API_KEY", "")
    config.get_settings.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="JEV_API_KEY"):
            async_client()
    finally:
        config.get_settings.cache_clear()


def test_check_answers_names_the_missing_question_keys():
    from pipelines.common.clients.jev import check_answers

    with pytest.raises(RuntimeError, match="c1_role"):
        check_answers({"c0_is_company": 1, "c1_role": 2}, {"c0_is_company": object()})


def test_check_answers_passes_when_every_question_is_answered():
    from pipelines.common.clients.jev import check_answers

    check_answers({"a": 1}, {"a": 2, "extra": 3})
