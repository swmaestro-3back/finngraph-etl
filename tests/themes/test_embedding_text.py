"""임베딩 입력 텍스트 조립 순수 로직 테스트."""

from __future__ import annotations

from pipelines.themes.jobs.embed_themes import reason_text, theme_text


def test_theme_text_joins_name_and_description():
    assert theme_text("2차전지", "배터리 밸류체인") == "2차전지\n배터리 밸류체인"


def test_theme_text_without_description_keeps_name_only():
    assert theme_text("2차전지", None) == "2차전지\n"


def test_reason_text_prefixes_theme_name():
    assert (
        reason_text("로봇", "정밀 제어용 기어드 모터 생산") == "[로봇] 정밀 제어용 기어드 모터 생산"
    )
