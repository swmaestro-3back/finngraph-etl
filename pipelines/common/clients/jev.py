"""jev(TypeSafe System One) 공용 헬퍼.

뉴스 관련성 필터와 엔티티 필터가 쓴다. jev 는 생성 모델이 아니라 Choice / Score / Noul 질문에
확률만 돌려주는 판정 모델이다 — 호출 모양은 pipelines.news.transformers.filters 의
relevance_filter·entity_filter 참조.
접속 설정(JEV_API_KEY, JEV_MODEL, 타임아웃)은 pipelines.common.config.Settings 가 담당한다.
typesafe_sdk 는 호출 시점에 import 한다 — DAG 파싱과 import 시점에는 비용이 없다.
"""

from __future__ import annotations

import math
from typing import Any

from pipelines.common.config import get_settings

RETRY_ATTEMPTS = 2


def async_client():
    """설정의 키·모델·타임아웃으로 만든 AsyncTypeSafeClient.

    커넥션 풀이 만든 이벤트 루프에 묶이므로, 루프 안에서 만들고 그 루프가 끝나기 전에 aclose() 로
    닫는다. asyncio.run 마다 루프가 새로 생기니 런을 넘겨 재사용하면 'Event loop is closed' 가
    난다 — judge 객체가 런마다 열고 닫는다(aclose_judge)."""
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


def check_answers(questions: dict[str, Any], answers: dict[str, Any]) -> None:
    """질문마다 답이 왔는지 확인한다. 빠진 키를 이름으로 올려 KeyError 보다 원인을 바로 보이게 한다.

    질문 키와 응답 키가 어긋나는 것은 모델 버전(jev-latest 별칭)이 바뀌었을 때 나타나는 증상이다."""
    missing = sorted(set(questions) - set(answers))
    if missing:
        shown = ", ".join(missing[:5]) + (" …" if len(missing) > 5 else "")
        raise RuntimeError(
            f"jev 응답에 질문 {len(missing)}/{len(questions)}개의 답이 없다"
            f" (모델 {get_settings().jev_model}): {shown}"
        )


async def aclose_judge(judge: Any) -> None:
    """judge 가 aclose 를 가지면(운영 jev judge) 닫는다. 테스트의 가짜 judge 는 함수라 그냥
    지난다."""
    close = getattr(judge, "aclose", None)
    if close is not None:
        await close()


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
