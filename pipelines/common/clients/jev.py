"""jev(TypeSafe System One) 공용 헬퍼.

뉴스 관련성 필터와 엔티티 필터가 쓴다. jev 는 생성 모델이 아니라 Choice / Score / Noul 질문에
확률만 돌려주는 판정 모델이다 — 호출 모양은 pipelines.news.transformers.filters 의
relevance_filter·entity_filter 참조.
접속 설정(JEV_API_KEY, JEV_MODEL, 타임아웃)은 pipelines.common.config.Settings 가 담당한다.
typesafe_sdk 는 호출 시점에 import 한다 — DAG 파싱과 import 시점에는 비용이 없다.
"""

from __future__ import annotations

import math

from pipelines.common.config import get_settings

RETRY_ATTEMPTS = 2


def async_client():
    """설정의 키·모델·타임아웃으로 만든 AsyncTypeSafeClient.

    이벤트 루프 안에서 만들어 재사용한다."""
    from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy

    settings = get_settings()
    if not settings.jev_api_key:
        raise RuntimeError("JEV_API_KEY 가 비어 있다")
    return AsyncTypeSafeClient(
        api_key=settings.jev_api_key,
        model=settings.jev_model,
        retry=RetryPolicy(max_retries=RETRY_ATTEMPTS, timeout=settings.jev_request_timeout),
        timeout=settings.jev_request_timeout,
    )


def _percentile(sorted_ms: list[float], p: float) -> float:
    """nearest-rank 백분위. 정렬된 입력을 받는다."""
    return sorted_ms[max(0, math.ceil(p * len(sorted_ms)) - 1)]


def _fmt(ms: float) -> str:
    return f"{ms:.0f}ms" if ms < 1000 else f"{ms / 1000:.1f}s"


def latency_summary(latencies_ms: list[float]) -> str:
    """청크 요약 로그에 붙는 jev 호출 지연 — "jev 1223건 p50 178ms p95 310ms 최대 1.4s".

    SDK 안의 재시도까지 포함한, 호출 한 번의 체감 시간이다. 요청마다 찍던 SDK INFO 로그를
    요청 로그는 logging.py 가 접거나 끄므로 응답 속도는 이 한 줄로 본다. 비어 있으면 "jev 0건"."""
    if not latencies_ms:
        return "jev 0건"
    ordered = sorted(latencies_ms)
    return (
        f"jev {len(ordered)}건 p50 {_fmt(_percentile(ordered, 0.5))}"
        f" p95 {_fmt(_percentile(ordered, 0.95))} 최대 {_fmt(ordered[-1])}"
    )
