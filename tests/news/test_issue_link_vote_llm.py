"""투표 판정 LLM 클라이언트 단위 테스트다. 캐시 키와 재사용, 검증과 형식 재요청, JSON 모드, 호출
상한, 비용을 확인한다."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError
from issue_link_vote_fakes import MemoryStore

from pipelines.news.transformers.issue_link_vote import llm
from pipelines.news.transformers.issue_link_vote.llm import (
    STAGES,
    Caps,
    IssueBudgetExceeded,
    LlmClient,
    RunBudgetExceeded,
)
from pipelines.news.transformers.prompts.issue_link_pair import EVIDENCE_TOOL

MODELS = {stage: "us.anthropic.claude-sonnet-4-6" for stage in STAGES}
MODELS["screen"] = "moonshotai.kimi-k2.5"


def _text(text: str, stop: str = "end_turn") -> dict:
    return {
        "output": {"message": {"content": [{"text": text}]}},
        "stopReason": stop,
        "usage": {"inputTokens": 1_000_000, "outputTokens": 100_000},
    }


class Scripted:
    """요청마다 다음 응답을 돌려준다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests: list[dict] = []

    def __call__(self, request):
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _client(invoke, store=None, run_cap=100, issue_cap=100) -> LlmClient:
    return LlmClient(
        MODELS,
        store or MemoryStore(),
        invoke,
        run_cap=run_cap,
        issue_cap=issue_cap,
        sleep=lambda _: None,
    )


def _screen(client: LlmClient, user: str = "pair"):
    return client.call(
        "screen", system="sys", user=user, max_tokens=400, cluster_id=2, candidate_id=1
    )


def test_free_text_reply_is_validated_and_cached_by_input():
    store = MemoryStore()
    invoke = Scripted(_text('앞말 {"label": "same_story"} 뒷말'))
    reply = _screen(_client(invoke, store))

    # enum 은 대소문자를 맞춰 스키마 표기로 바꾼다.
    assert reply.obj == {"label": "SAME_STORY"}
    assert reply.parser_text() == '{"label": "SAME_STORY"}'
    assert reply.fresh_calls == 1
    record = next(iter(store.rows.values()))
    assert (record.stage, record.model, record.prompt_version) == (
        "screen",
        "moonshotai.kimi-k2.5",
        "pair-screen-v1",
    )
    assert (record.cluster_id, record.candidate_cluster_id) == (2, 1)
    # 요청 원문은 남기지 않는다.
    assert "pair" not in json.dumps(record.response, ensure_ascii=False)

    # 같은 입력이면 다음 실행(새 클라이언트)도 캐시를 쓴다.
    again = _client(Scripted(), store)
    reply2 = _screen(again)
    assert reply2.obj == {"label": "SAME_STORY"}
    assert (again.stats.calls, again.stats.cache_hits) == (0, 1)
    # 입력이 바뀌면 새로 부른다.
    changed = _client(Scripted(_text('{"label": "DIFFERENT"}')), store)
    assert _screen(changed, "pair changed").obj == {"label": "DIFFERENT"}
    assert changed.stats.calls == 1


def test_key_covers_model_stage_version_sample_and_request():
    req, _ = LlmClient.wire("m", "s", "u", 10, 0.0, None)
    base = LlmClient.key("m", "screen", 0, req)
    assert base != LlmClient.key("m2", "screen", 0, req)
    assert base != LlmClient.key("m", "screen", 1, req)
    assert base != LlmClient.key("m", "judge", 0, req)
    req2, _ = LlmClient.wire("m", "s", "u2", 10, 0.0, None)
    assert base != LlmClient.key("m", "screen", 0, req2)


def test_invalid_reply_gets_one_repair_then_conservative_answer():
    invoke = Scripted(_text("모르겠어요"), _text('{"label": "MAYBE"}'))
    client = _client(invoke)
    reply = _screen(client)

    assert reply.obj is None
    assert reply.parser_text() == ""
    assert reply.retries == 1
    assert client.stats.calls == 2
    assert client.stats.parse_failures == 1
    # 형식 재요청은 앞 답을 assistant 로 붙이고 지시를 더한다.
    repair = invoke.requests[1]["messages"]
    assert [m["role"] for m in repair] == ["user", "assistant", "user"]
    assert "could not be used" in repair[2]["content"][0]["text"]


def test_repair_success_is_used():
    invoke = Scripted(_text(""), _text('{"label": "SAME_EVENT"}'))
    assert _screen(_client(invoke)).obj == {"label": "SAME_EVENT"}


def test_tool_stage_reads_tool_input_and_ignores_reasoning():
    response = {
        "output": {
            "message": {
                "content": [
                    {"reasoningContent": {"reasoningText": {"text": "생각"}}},
                    {
                        "toolUse": {
                            "name": "record_verdict",
                            "input": {
                                "matter_in_a": "a",
                                "matter_in_b": "b",
                                "same_specific_matter": "true",
                                "label": "SAME_STORY",
                                "reason": "r",
                            },
                        }
                    },
                ]
            }
        },
        "stopReason": "tool_use",
        "usage": {},
    }
    invoke = Scripted(response)
    client = _client(invoke)
    reply = client.call(
        "verify.evidence",
        system="s",
        user="u",
        max_tokens=800,
        temperature=1.0,
        sample=1,
        tool=EVIDENCE_TOOL,
        cluster_id=2,
        candidate_id=1,
    )

    assert reply.obj["same_specific_matter"] is True
    assert reply.tool_row()["stop_reason"] == "tool_use"
    request = invoke.requests[0]
    assert request["toolConfig"]["toolChoice"] == {"tool": {"name": "record_verdict"}}
    assert request["inferenceConfig"] == {"maxTokens": 800, "temperature": 1.0}


def test_json_mode_for_models_without_forced_tools(monkeypatch):
    monkeypatch.setitem(llm.MODEL_CAPS, "us.anthropic.claude-sonnet-4-6", Caps(tool_choice=False))
    payload = {
        "matter_in_a": "a",
        "matter_in_b": "none",
        "same_specific_matter": False,
        "label": "DIFFERENT",
        "reason": "r",
    }
    invoke = Scripted(_text("<think>고민</think>" + json.dumps({"input": payload})))
    client = _client(invoke)
    reply = client.call(
        "verify.evidence", system="s", user="u", max_tokens=800, tool=EVIDENCE_TOOL, cluster_id=2
    )

    assert reply.adapter == "json"
    assert reply.obj["label"] == "DIFFERENT"
    request = invoke.requests[0]
    assert "toolConfig" not in request
    assert "No tool can be called" in request["messages"][0]["content"][0]["text"]


def test_refused_request_is_cached_as_conservative_and_throttling_is_retried():
    throttled = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "slow"}}, "Converse"
    )
    too_long = ClientError(
        {"Error": {"Code": "ValidationException", "Message": "too long"}}, "Converse"
    )
    invoke = Scripted(throttled, too_long, too_long)
    client = _client(invoke)
    reply = _screen(client)

    assert reply.obj is None
    assert "request refused" in reply.error
    assert len(invoke.requests) == 3


def test_other_bedrock_errors_raise_without_caching():
    store = MemoryStore()
    denied = ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "Converse")
    with pytest.raises(RuntimeError, match="AccessDeniedException"):
        _screen(_client(Scripted(denied), store))
    assert store.rows == {}


def test_budgets_count_only_fresh_calls():
    store = MemoryStore()
    client = _client(
        Scripted(*[_text('{"label": "DIFFERENT"}')] * 5), store, run_cap=3, issue_cap=2
    )
    client.begin_issue()
    _screen(client, "a")
    _screen(client, "b")
    # 캐시 적중은 상한에 들지 않는다.
    _screen(client, "a")
    with pytest.raises(IssueBudgetExceeded):
        _screen(client, "c")
    client.begin_issue()
    _screen(client, "c")
    assert not client.run_budget_left()
    with pytest.raises(RunBudgetExceeded):
        _screen(client, "d")
    # 이미 받은 응답은 상한에 닿은 뒤에도 쓸 수 있다.
    assert _screen(client, "b").obj == {"label": "DIFFERENT"}


def test_cost_estimate_uses_price_table():
    client = _client(Scripted(_text('{"label": "DIFFERENT"}')))
    _screen(client)
    # Kimi K2.5 단가로 입력 100만 토큰은 $0.60, 출력 10만 토큰은 $0.30 이다.
    assert client.stats.cost_usd == pytest.approx(0.90)
    assert (client.stats.input_tokens, client.stats.output_tokens) == (1_000_000, 100_000)


def test_every_stage_needs_a_model():
    with pytest.raises(ValueError, match="모델이 정해지지 않은 단계"):
        LlmClient({"screen": "m"}, MemoryStore(), Scripted(), run_cap=1, issue_cap=1)
