"""summarizer 순수 함수 + vLLM async 골격(모킹) 단위 테스트.

외부 LLM/DB에 의존하지 않도록 httpx.AsyncClient를 가짜로 대체한다.
pytest-asyncio 의존을 피하기 위해 async 경로는 asyncio.run으로 감싸 동기 테스트로 실행한다.
"""

from __future__ import annotations

import asyncio

import pytest

from pipelines.news.transformers import summarizer

# ── 프롬프트 파일 스텁 ────────────────────────────────────────────────────
# prompts/ 디렉토리는 .gitignore 대상이라 CI에는 실제 파일이 없다.
# 실제 템플릿과 같은 $title/$triplets/$source_text 플레이스홀더 구조를 유지한다.

_FAKE_PROMPT_TEXTS = {
    "summary_single.txt": (
        "[Title]\n$title\n\n[Entity relations]\n$triplets\n\n[Body]\n$source_text\n\n[Summary]"
    ),
    "summary_single_system.txt": "You are a financial news summarizer.",
}


def _stub_prompt_loading(monkeypatch):
    monkeypatch.setattr(
        summarizer, "load_summary_prompt_text", lambda filename: _FAKE_PROMPT_TEXTS[filename]
    )


# ── format_triplets_for_prompt ────────────────────────────────────────────


def test_format_triplets_empty_returns_none_marker():
    assert summarizer.format_triplets_for_prompt([]) == "(none)"
    assert summarizer.format_triplets_for_prompt(None) == "(none)"


def test_format_triplets_renders_arrow_lines():
    text = summarizer.format_triplets_for_prompt([("삼성전자", "실적발표", "어닝서프라이즈")])
    assert text == "- 삼성전자 —[실적발표]→ 어닝서프라이즈"


def test_format_triplets_dedups_and_skips_malformed():
    text = summarizer.format_triplets_for_prompt(
        [
            ("삼성전자", "공급하다", "애플"),
            ("삼성전자", "공급하다", "애플"),  # 중복 → 1회만
            ("", "관계", "대상"),  # subject 비어 skip
            ("주체", "관계"),  # 길이 부족 skip
        ]
    )
    assert text == "- 삼성전자 —[공급하다]→ 애플"


# ── parse_summary_response ────────────────────────────────────────────────


def test_parse_summary_strips_think_and_fences():
    payload = {
        "choices": [
            {"message": {"content": "<think>추론</think>```삼성전자가 실적을 발표했다.```"}}
        ]
    }
    assert summarizer.parse_summary_response(payload) == "삼성전자가 실적을 발표했다."


def test_parse_summary_empty_choices_raises():
    with pytest.raises(RuntimeError):
        summarizer.parse_summary_response({"choices": []})


def test_parse_summary_empty_content_returns_empty_string():
    payload = {"choices": [{"message": {"content": "   "}}]}
    assert summarizer.parse_summary_response(payload) == ""


# ── build_summary_prompt (Template.safe_substitute) ───────────────────────


def test_build_summary_prompt_injects_fields_and_survives_dollar_in_body(monkeypatch):
    _stub_prompt_loading(monkeypatch)
    item = {
        "title": "삼성전자 실적 발표",
        "_body_text": "삼성전자가 3분기 영업이익 $10B를 기록했다.",  # 본문의 $가 렌더를 깨면 안 됨
        "_triplets": [("삼성전자", "실적발표", "어닝서프라이즈")],
    }
    prompt = summarizer.build_summary_prompt(item)
    assert "삼성전자 실적 발표" in prompt
    assert "삼성전자 —[실적발표]→ 어닝서프라이즈" in prompt
    assert "$10B" in prompt  # safe_substitute가 미지정 $를 보존


# ── build_summaries_async (httpx 모킹) ────────────────────────────────────


class _FakeResponse:
    def __init__(self, content: str):
        self._content = content

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._content}}]}


class _FakeAsyncClient:
    """title에 'FAIL'이 있으면 예외, 아니면 고정 요약을 반환하는 가짜 클라이언트."""

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, headers=None, json=None, timeout=None):
        user_content = json["messages"][1]["content"]
        if "FAIL" in user_content:
            raise RuntimeError("모킹된 vLLM 실패")
        return _FakeResponse("요약 결과 문장.")


def _run_summaries(items, monkeypatch):
    _stub_prompt_loading(monkeypatch)
    monkeypatch.setattr(summarizer.httpx, "AsyncClient", _FakeAsyncClient)
    config = {
        "vllm_base_url": "http://vllm.local",
        "vllm_model": "test-model",
        "vllm_api_key": "EMPTY",
        "vllm_timeout": 10,
        "body_limit": 12000,
        "max_tokens": 512,
    }
    return asyncio.run(
        summarizer.build_summaries_async(items=items, config=config, max_concurrency=2)
    )


def test_build_summaries_returns_id_summary_pairs(monkeypatch):
    items = [
        {"_news_id": 1, "title": "OK 뉴스", "_body_text": "본문 내용", "_triplets": []},
        {"_news_id": 2, "title": "다른 OK 뉴스", "_body_text": "본문", "_triplets": []},
    ]
    results = _run_summaries(items, monkeypatch)
    assert sorted(results) == [(1, "요약 결과 문장."), (2, "요약 결과 문장.")]


def test_build_summaries_drops_failures(monkeypatch):
    items = [
        {"_news_id": 1, "title": "OK 뉴스", "_body_text": "본문", "_triplets": []},
        {"_news_id": 2, "title": "FAIL 뉴스", "_body_text": "본문", "_triplets": []},
    ]
    results = _run_summaries(items, monkeypatch)
    assert results == [(1, "요약 결과 문장.")]  # 실패분(2)은 미커밋 → 제외


def test_build_summaries_empty_input(monkeypatch):
    assert _run_summaries([], monkeypatch) == []
