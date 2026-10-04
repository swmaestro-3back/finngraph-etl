"""요약 프롬프트 조립·출력 검증 단위 테스트 (Bedrock 호출 없음)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipelines.news.transformers.prompts import summary as summary_prompt
from pipelines.news.transformers.summarizer import (
    KeyPoint,
    NewsSummary,
    PointKind,
    SummaryInvalid,
    build_summary_prompt,
    format_published_date,
    summarize_news_items,
    validate_summary,
)

SUMMARY = "반도체 장비 수출 규제가 11월 1일부터 시행돼요. 한빛반도체는 국산 소재 조달을 늘려요."


def _point(kind: str, text: str = "규제 대상 장비의 해외 반출이 막혀요.") -> KeyPoint:
    return KeyPoint(kind=PointKind(kind), text=text)


def _draft(*kinds: str, summary: str = SUMMARY) -> NewsSummary:
    return NewsSummary(summary=summary, key_points=[_point(kind) for kind in kinds])


def _item(news_id: int, text: str) -> dict:
    return {
        "_news_id": news_id,
        "title": "제목",
        "_text": text,
        "_published_at": datetime(2026, 9, 9, 1, tzinfo=UTC),
    }


def test_format_published_date_uses_seoul_day():
    # UTC 9월 30일 16시는 서울 기준 10월 1일
    assert format_published_date(datetime(2026, 9, 30, 16, tzinfo=UTC)) == "2026년 10월 1일"


def test_format_published_date_missing():
    assert format_published_date(None) == "미상"


def test_build_summary_prompt_includes_published_date():
    item = {
        "title": "제목",
        "_text": "본문 내용",
        "_published_at": datetime(2026, 9, 9, 1, tzinfo=UTC),
    }

    prompt = build_summary_prompt(item)

    assert "[발행일]\n2026년 9월 9일" in prompt
    assert "$published_date" not in prompt


def test_system_prompt_defines_every_point_kind():
    # 스키마의 enum 과 프롬프트의 항목 정의가 어긋나면 모델이 정의 없는 항목을 고른다
    for kind in PointKind:
        assert f"- {kind.value} (" in summary_prompt.SYSTEM


def test_validate_summary_cleans_and_orders_points():
    draft = NewsSummary(
        summary=f"  {SUMMARY}\n",
        key_points=[
            _point("RIPPLE", " 협력사로 주문이 옮겨 갈 수 있어요. "),
            _point("CHANGE"),
            _point("AFFECTED", "규제 대상 장비를 쓰는 회사예요"),
        ],
    )

    result = validate_summary(draft)

    assert result.summary == SUMMARY
    # 모델이 낸 순서가 아니라 고정된 표시 순서다
    assert [point.kind for point in result.key_points] == [
        PointKind.CHANGE,
        PointKind.AFFECTED,
        PointKind.RIPPLE,
    ]
    assert result.key_points[2].text == "협력사로 주문이 옮겨 갈 수 있어요."


def test_validate_summary_allows_no_points_for_eventless_article():
    assert validate_summary(_draft()).key_points == []


@pytest.mark.parametrize(
    "draft",
    [
        _draft("CHANGE"),  # 1개
        _draft("CHANGE", "AFFECTED", "SCALE", "RIPPLE"),  # 4개
        _draft("CHANGE", "CHANGE"),  # 같은 항목 두 번
        _draft("CHANGE", "SCALE", summary="수출 규제가 11월 1일부터 시행된다."),  # 했다체 요약
        NewsSummary(
            summary=SUMMARY,
            key_points=[_point("CHANGE"), _point("SCALE", "계약 금액은 4970억원이다.")],
        ),  # 했다체 포인트
        _draft("CHANGE", "SCALE", summary="   "),  # 빈 요약
    ],
)
def test_validate_summary_rejects_rule_violations(draft):
    with pytest.raises(SummaryInvalid):
        validate_summary(draft)


def test_summarize_news_items_retries_once_and_isolates_failures():
    calls: list[str] = []

    async def summarizer(text):
        calls.append(text)
        if "실패" in text:
            raise RuntimeError("bedrock down")
        if "[재요청]" in text:
            return _draft("SCALE", "CHANGE")
        if "한번위반" in text or "계속위반" in text:
            return _draft("CHANGE")
        return _draft("CHANGE", "AFFECTED")

    rows = summarize_news_items(
        [
            _item(1, "정상 본문"),
            _item(2, "실패 본문"),
            _item(3, "한번위반 본문"),
        ],
        summarizer=summarizer,
        max_concurrency=2,
    )

    assert sorted(rows) == [
        (
            1,
            SUMMARY,
            [
                {"kind": "CHANGE", "text": "규제 대상 장비의 해외 반출이 막혀요."},
                {"kind": "AFFECTED", "text": "규제 대상 장비의 해외 반출이 막혀요."},
            ],
        ),
        (
            3,
            SUMMARY,
            [
                {"kind": "CHANGE", "text": "규제 대상 장비의 해외 반출이 막혀요."},
                {"kind": "SCALE", "text": "규제 대상 장비의 해외 반출이 막혀요."},
            ],
        ),
    ]
    # 1·2·3 첫 호출 + 3 재요청. 재요청에는 같은 본문과 어긴 규칙이 실린다
    assert len(calls) == 4
    [retry] = [call for call in calls if "[재요청]" in call]
    assert "한번위반 본문" in retry
    assert "1개" in retry


def test_summarize_news_items_drops_article_still_invalid_after_retry():
    async def summarizer(text):
        return _draft("CHANGE")

    assert summarize_news_items([_item(1, "계속위반 본문")], summarizer=summarizer) == []


def test_summarize_news_items_empty_input_skips_llm():
    async def summarizer(text):  # pragma: no cover
        raise AssertionError("호출되면 안 된다")

    assert summarize_news_items([], summarizer=summarizer) == []
