"""클러스터 이름 생성 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipelines.news.repositories.news_clusters import ClusterArticle
from pipelines.news.transformers.cluster_titler import (
    ClusterTitle,
    TitleTooLong,
    build_title_input,
    load_system_prompt,
    title_clusters,
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


def test_load_system_prompt_injects_max_chars():
    prompt = load_system_prompt(25)

    assert "25자 이하" in prompt
    assert "{max_chars}" not in prompt


def test_title_clusters_isolates_failures_and_skips_empty():
    calls: list[str] = []

    async def titler(text):
        calls.append(text)
        if "실패" in text:
            raise RuntimeError("bedrock down")
        if "긴제목" in text:
            return ClusterTitle(title="가" * 40)  # 축약 재요청에도 여전히 길다
        if "[재요청]" in text:
            return ClusterTitle(title="네오볼타 ESS 공급계약")
        if "축약" in text:
            return ClusterTitle(title="네오볼타 ESS 배터리 공급계약 체결 공시 발표")
        return ClusterTitle(title=" 노바티스 피하주사 계약 ")

    titles = title_clusters(
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

    assert titles == {1: "노바티스 피하주사 계약", 5: "네오볼타 ESS 공급계약"}
    # 1·2·3·5 첫 호출 + 3·5 축약 재요청
    assert len(calls) == 6
    shorten = [c for c in calls if "[재요청]" in c]
    assert len(shorten) == 2
    assert any("20자를 넘는다" in c and "공시 발표" in c for c in shorten)


def test_title_clusters_empty_input_skips_llm():
    async def titler(text):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    assert title_clusters({}, titler=titler) == {}
