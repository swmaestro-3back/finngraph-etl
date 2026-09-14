"""generator 순수 함수: 프롬프트 입력 조립과 당사자 검증."""

from __future__ import annotations

from datetime import date

from pipelines.events.models import EventDraft
from pipelines.events.transformers.generator import build_prompt_input, validate_draft


def test_build_prompt_input_blocks_and_candidates():
    dated_texts = [
        (date(2026, 9, 1), "삼성전자 유상증자\n삼성전자가 결의했다."),
        (None, "기아 리콜\n기아가 리콜한다."),
    ]

    rendered = build_prompt_input(dated_texts, ["삼성전자", "기아"])

    assert "[기사 1] 2026-09-01 | 삼성전자 유상증자\n삼성전자가 결의했다." in rendered
    assert "[기사 2] 날짜 미상 | 기아 리콜\n기아가 리콜한다." in rendered
    assert rendered.endswith("[후보 기업]\n- 삼성전자\n- 기아")


def test_build_prompt_input_text_without_body():
    rendered = build_prompt_input([(date(2026, 9, 1), "제목만")], ["삼성전자"])

    assert "[기사 1] 2026-09-01 | 제목만\n\n[후보 기업]" in rendered


def test_validate_keeps_candidates_only_in_order_without_duplicates(caplog):
    draft = EventDraft(companies=["기아", "엔비디아", "삼성전자", "기아"])

    validated = validate_draft(draft, ["삼성전자", "기아"])

    assert validated.companies == ["기아", "삼성전자"]
    assert "엔비디아" in caplog.text


def test_validate_empty_companies_is_allowed():
    assert validate_draft(EventDraft(companies=[]), ["삼성전자"]).companies == []
