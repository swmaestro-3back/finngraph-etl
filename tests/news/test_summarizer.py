"""요약 프롬프트 조립 단위 테스트 (Bedrock 호출 없음)."""

from __future__ import annotations

from datetime import UTC, datetime

from pipelines.news.transformers.summarizer import build_summary_prompt, format_published_date


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
