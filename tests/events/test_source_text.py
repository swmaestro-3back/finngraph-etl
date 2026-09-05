"""멤버 기사 → LLM 입력 텍스트."""

from __future__ import annotations

from datetime import UTC, date, datetime

from pipelines.events.models import MemberArticle
from pipelines.events.transformers.source_text import member_date, member_source_text


def _member(**overrides) -> MemberArticle:
    base = {
        "news_id": 1,
        "title": "삼성전자,  4조원 규모\n유상증자 결정",
        "summary": None,
        "text": "본문 " * 500,
        "published_at": None,
    }
    base.update(overrides)
    return MemberArticle(**base)


def test_uses_summary_when_present():
    member = _member(summary="  삼성전자가 유상증자를 결의했다.  ")

    assert member_source_text(member, lead_chars=600) == (
        "삼성전자, 4조원 규모 유상증자 결정\n삼성전자가 유상증자를 결의했다."
    )


def test_blank_summary_falls_back_to_lead():
    member = _member(summary="   ", text="가 나 다 라 마")

    assert member_source_text(member, lead_chars=3) == "삼성전자, 4조원 규모 유상증자 결정\n가 나"


def test_lead_is_cut_after_whitespace_collapse():
    member = _member(text="첫줄\n\n둘째줄   셋째")

    assert (
        member_source_text(member, lead_chars=6)
        == "삼성전자, 4조원 규모 유상증자 결정\n첫줄 둘째줄"
    )


def test_short_body_is_not_padded():
    member = _member(text="짧다")

    assert member_source_text(member, lead_chars=600).endswith("\n짧다")


def test_member_date_converts_to_kst():
    # UTC 9/1 20:00 = KST 9/2 05:00
    member = _member(published_at=datetime(2026, 9, 1, 20, 0, tzinfo=UTC))

    assert member_date(member) == date(2026, 9, 2)


def test_member_date_none():
    assert member_date(_member(published_at=None)) is None
