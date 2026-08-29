"""material_event_filter 프롬프트 생성 단위 테스트 (API/DB 불필요)."""

from __future__ import annotations

from pipelines.news.transformers.material_event_filter import build_llm_material_event_prompt


def test_build_llm_material_event_prompt_fills_description():
    """템플릿의 $description 자리표시자가 item의 description으로 채워져야 한다.

    빌더가 description을 넘기지 않아 Template.substitute가 KeyError를 던지고,
    fail_open=False에서 전 기사가 LLM 판정 없이 드랍되던 회귀를 잡는다.
    """

    item = {
        "title": "삼성전자 유상증자 결정",
        "description": "삼성전자가 1조원 규모 유상증자를 결정했다",
        "_text": "삼성전자는 이사회를 열고 유상증자를 결의했다고 공시했다.",
    }

    prompt = build_llm_material_event_prompt(item)

    assert "삼성전자 유상증자 결정" in prompt
    assert "삼성전자가 1조원 규모 유상증자를 결정했다" in prompt


def test_build_llm_material_event_prompt_handles_missing_description():
    """DB 조회 경로가 아닌 곳에서 description 키 자체가 없어도 렌더링돼야 한다."""

    item = {"title": "현대차 리콜 확대", "_text": "현대차가 리콜 대상을 확대했다."}

    prompt = build_llm_material_event_prompt(item)

    assert "현대차 리콜 확대" in prompt
