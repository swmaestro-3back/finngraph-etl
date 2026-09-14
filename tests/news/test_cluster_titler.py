"""클러스터 이름 생성 단위 테스트. Bedrock 은 부르지 않는다."""

from __future__ import annotations

from datetime import datetime

import pytest

from pipelines.news.repositories.news_clusters import ClusterArticle
from pipelines.news.transformers.cluster_titler import (
    ClusterTitle,
    build_title_input,
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
    with pytest.raises(ValueError):
        validate_cluster_title("아" * 31, 30)


def test_title_clusters_isolates_failures_and_skips_empty():
    calls: list[str] = []

    async def titler(text):
        calls.append(text)
        if "실패" in text:
            raise RuntimeError("bedrock down")
        if "긴제목" in text:
            return ClusterTitle(title="가" * 40)
        return ClusterTitle(title=" 노바티스 피하주사 계약 ")

    titles = title_clusters(
        {
            1: [_article("알테오젠, 노바티스 계약")],
            2: [_article("실패 기사")],
            3: [_article("긴제목 기사")],
            4: [],  # 멤버 없음 → 호출 안 함
        },
        titler=titler,
        max_concurrency=2,
        max_chars=30,
    )

    assert titles == {1: "노바티스 피하주사 계약"}
    assert len(calls) == 3


def test_title_clusters_empty_input_skips_llm():
    async def titler(text):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    assert title_clusters({}, titler=titler) == {}
