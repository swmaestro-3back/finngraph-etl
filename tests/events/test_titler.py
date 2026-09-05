"""프롬프트 입력 렌더와 LLM 출력 검증. LLM 은 부르지 않는다."""

from __future__ import annotations

import logging
from datetime import date

import pytest

from pipelines.events.models import EventDraft
from pipelines.events.transformers.titler import render_prompt_input, validate_draft


def test_render_prompt_input_blocks_and_candidates():
    dated_texts = [
        (date(2026, 9, 1), "삼성전자, 4조원 유상증자 결정\n삼성전자가 결의했다."),
        (None, "삼성전자 주가 급락\n유상증자 소식에 급락."),
    ]

    rendered = render_prompt_input(dated_texts, ["삼성전자", "기아"])

    assert rendered == (
        "[기사 1] 2026-09-01 | 삼성전자, 4조원 유상증자 결정\n삼성전자가 결의했다.\n\n"
        "[기사 2] 날짜 미상 | 삼성전자 주가 급락\n유상증자 소식에 급락.\n\n"
        "[후보 기업]\n- 삼성전자\n- 기아"
    )


def test_render_prompt_input_text_without_body():
    rendered = render_prompt_input([(date(2026, 9, 1), "제목만")], ["삼성전자"])

    assert rendered.startswith("[기사 1] 2026-09-01 | 제목만\n\n[후보 기업]")


def test_validate_keeps_only_candidates_in_llm_order_without_duplicates(caplog):
    draft = EventDraft(
        companies=["기아", "엔비디아", "삼성전자", "기아"], title="기아, 엔비디아와 협력"
    )

    with caplog.at_level(logging.WARNING):
        validated = validate_draft(draft, ["삼성전자", "기아"], max_chars=60)

    assert validated.companies == ["기아", "삼성전자"]
    assert "엔비디아" in caplog.text


def test_validate_strips_leading_tags_and_collapses_whitespace():
    draft = EventDraft(companies=[], title="[속보]  삼성전자,   4조원\n유상증자 결정 ")

    assert validate_draft(draft, [], max_chars=60).title == "삼성전자, 4조원 유상증자 결정"


def test_validate_rejects_empty_title():
    with pytest.raises(ValueError):
        validate_draft(EventDraft(companies=[], title="[속보] "), [], max_chars=60)


def test_validate_rejects_too_long_title():
    with pytest.raises(ValueError):
        validate_draft(EventDraft(companies=[], title="가" * 61), [], max_chars=60)


def test_validate_allows_max_length_exactly():
    assert (
        validate_draft(EventDraft(companies=[], title="가" * 60), [], max_chars=60).title
        == "가" * 60
    )
