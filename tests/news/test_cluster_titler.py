"""클러스터 이름 생성 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipelines.news.repositories.news_clusters import ClusterArticle
from pipelines.news.transformers.cluster_titler import (
    ClusterLabel,
    ClusterTitle,
    SummaryTooLong,
    TitleTooLong,
    build_title_input,
    load_system_prompt,
    pick_summary,
    summarize_titled_clusters,
    title_clusters,
    validate_cluster_summary,
    validate_cluster_title,
)
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE


def _article(title: str, text: str = "본문", day: int | None = 9) -> ClusterArticle:
    published = datetime(2026, 9, day, 10, tzinfo=SEOUL_TIMEZONE) if day else None
    return ClusterArticle(title=title, text=text, published_at=published)


def test_build_title_input_numbers_articles_with_date_and_lead():
    text = build_title_input(
        [_article("엘앤에프, 삼성SDI 공급", "본문  첫째 줄\n둘째 줄"), _article("후속", day=None)],
        lead_chars=8,
    )

    assert (
        text
        == "[기사 1] 2026-09-09 | 엘앤에프, 삼성SDI 공급\n본문 첫째 줄\n\n[기사 2] 날짜 미상 | 후속\n본문"
    )


def test_validate_cluster_title_cleans_and_bounds():
    assert (
        validate_cluster_title("  [속보] 삼성SDI  양극재 공급계약 ", 30)
        == "삼성SDI 양극재 공급계약"
    )

    with pytest.raises(ValueError):
        validate_cluster_title("[특징주]", 30)
    with pytest.raises(TitleTooLong):
        validate_cluster_title("아" * 31, 30)


def test_validate_cluster_summary_collapses_and_bounds():
    assert (
        validate_cluster_summary(" 미국 8월 건설지출이\n 전월 대비 0.9% 늘었어요. ", 30)
        == "미국 8월 건설지출이 전월 대비 0.9% 늘었어요."
    )

    with pytest.raises(ValueError):
        validate_cluster_summary("  \n ", 30)
    with pytest.raises(ValueError):
        validate_cluster_summary(None, 30)
    with pytest.raises(SummaryTooLong):
        validate_cluster_summary("요" * 31, 30)


def test_pick_summary_takes_first_valid_and_drops_invalid():
    assert pick_summary(["", "실적이 늘었어요."], 30, cluster_id=1) == "실적이 늘었어요."
    assert pick_summary(["가" * 31, "짧아요."], 30, cluster_id=1) == "짧아요."
    # 자르지 않고 버린다
    assert pick_summary(["가" * 31], 30, cluster_id=1) is None
    assert pick_summary([None, "  "], 30, cluster_id=1) is None


def test_load_system_prompt_injects_max_chars():
    prompt = load_system_prompt(25, 150)

    assert "25자 이하" in prompt
    assert "150자 이하" in prompt
    assert "{max_chars}" not in prompt
    assert "{summary_max_chars}" not in prompt
    assert "[요약]" in prompt


def test_cluster_title_schema_requires_summary_but_parsing_tolerates_missing():
    schema = ClusterTitle.model_json_schema()

    # 구조화 출력 스키마에서는 필수 — 모델이 늘 요약을 쓰게 한다
    assert schema["required"] == ["title", "summary"]
    assert "default" not in schema["properties"]["summary"]
    # 응답에 요약이 빠져도 파싱은 되어 제목을 살린다
    assert ClusterTitle.model_validate_json('{"title": "현금배당 결정"}').summary == ""


def test_title_clusters_isolates_failures_and_skips_empty():
    calls: list[str] = []

    async def titler(text):
        calls.append(text)
        if "실패" in text:
            raise RuntimeError("bedrock down")
        if "긴제목" in text:
            return ClusterTitle(title="가" * 40)  # 축약 재요청에도 여전히 길다
        if "[재요청]" in text:
            return ClusterTitle(title="네오볼타 ESS 공급계약", summary="재요청 요약이에요.")
        if "축약" in text:
            return ClusterTitle(
                title="네오볼타 ESS 배터리 공급계약 체결 공시 발표", summary="첫 요약이에요."
            )
        return ClusterTitle(
            title=" 노바티스 피하주사 계약 ", summary=" 알테오젠이  노바티스와 계약했어요. "
        )

    labels = title_clusters(
        {
            1: [_article("알테오젠, 노바티스 계약")],
            2: [_article("실패 기사")],
            3: [_article("긴제목 기사")],
            4: [],  # 멤버 없음 → 호출 안 함
            5: [_article("축약 기사")],  # 한 번 넘치고 재요청에서 줄어든다
        },
        titler=titler,
        max_concurrency=2,
        max_chars=20,
    )

    assert labels == {
        1: ClusterLabel("노바티스 피하주사 계약", "알테오젠이 노바티스와 계약했어요."),
        # 재요청 응답의 요약을 쓴다
        5: ClusterLabel("네오볼타 ESS 공급계약", "재요청 요약이에요."),
    }
    # 1·2·3·5 첫 호출 + 3·5 축약 재요청
    assert len(calls) == 6
    shorten = [c for c in calls if "[재요청]" in c]
    assert len(shorten) == 2
    assert any("20자를 넘는다" in c and "공시 발표" in c for c in shorten)


def test_title_clusters_empty_input_skips_llm():
    async def titler(text):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    assert title_clusters({}, titler=titler) == {}


def test_title_clusters_keeps_title_when_summary_invalid():
    async def titler(text):
        if "긴요약" in text:
            return ClusterTitle(title="현금배당 결정", summary="요" * 31)
        if "빈요약" in text:
            return ClusterTitle(title="자사주 소각 결정", summary="  ")
        if "[재요청]" in text:
            return ClusterTitle(title="수주", summary="")  # 재요청은 요약이 비었다
        return ClusterTitle(title="남부발전 수주 공시 발표 일정", summary="첫 응답 요약이에요.")

    labels = title_clusters(
        {
            1: [_article("긴요약 기사")],
            2: [_article("빈요약 기사")],
            3: [_article("재요청 기사")],
        },
        titler=titler,
        max_chars=10,
        summary_max_chars=30,
    )

    assert labels == {
        1: ClusterLabel("현금배당 결정", None),
        2: ClusterLabel("자사주 소각 결정", None),
        # 재요청 요약이 비면 첫 응답 요약으로 돌아간다
        3: ClusterLabel("수주", "첫 응답 요약이에요."),
    }


def test_summarize_titled_clusters_hints_existing_title_and_keeps_only_summaries():
    calls: list[str] = []

    async def titler(text):
        calls.append(text)
        if "실패" in text:
            raise RuntimeError("bedrock down")
        if "긴요약" in text:
            return ClusterTitle(title="무시", summary="요" * 31)
        return ClusterTitle(title="새 제목은 버린다", summary="8월 건설지출이 0.9% 늘었어요.")

    summaries = summarize_titled_clusters(
        {
            1: ("미국 8월 건설지출", [_article("美 8월 건설지출 0.9% 증가")]),
            2: ("실패", [_article("실패 기사")]),
            3: ("긴요약", [_article("긴요약 기사")]),
            4: ("멤버 없음", []),  # 호출 안 함
        },
        titler=titler,
        summary_max_chars=30,
    )

    assert summaries == {1: "8월 건설지출이 0.9% 늘었어요."}
    assert len(calls) == 3
    # 기존 제목을 알려 줘 같은 사건을 요약하게 한다
    assert any("[기존 제목] '미국 8월 건설지출'" in c for c in calls)
    assert all("[재요청]" not in c for c in calls)
