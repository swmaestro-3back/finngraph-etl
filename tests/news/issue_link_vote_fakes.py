"""투표 판정 테스트에 쓰는 Bedrock Converse 와 LLM 응답 캐시의 가짜 구현이다.

FakeBedrock 은 요청의 시스템 프롬프트로 단계를 알아내고, 사용자 메시지에 나온 이슈 제목으로 쌍을
알아낸 뒤 정해 둔 답(PairScript)을 돌려준다. 정해 둔 답은 (부모 제목, 자식 제목) 쌍마다 투표자별로
적는다. 정해 둔 답이 없는 쌍은 모든 단계가 DIFFERENT 다. 실제 모델처럼 도구 단계는 toolUse 로, 자유
텍스트 단계는 JSON 본문으로 답한다.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from pipelines.news.transformers.issue_link_vote.llm import CallRecord
from pipelines.news.transformers.prompts import issue_kind as kind_prompt
from pipelines.news.transformers.prompts import issue_link_confirm as confirm_prompt
from pipelines.news.transformers.prompts import issue_link_pair as pair_prompt
from pipelines.news.transformers.prompts.issue_link_rank import SYSTEM_V8

SYSTEM_STAGES = {
    pair_prompt.SCREEN_PROMPT: "screen",
    pair_prompt.EVIDENCE_TOOL_PROMPT: "verify.evidence",
    pair_prompt.MATTER_PROMPT: "verify.matter",
    pair_prompt.PLAN_EXTRACT_PROMPT: "plan_extract",
    pair_prompt.PLANSTEP_PROMPT: "plan_step.planstep",
    pair_prompt.EVIDENCE3_TOOL_PROMPT: "plan_step.evidence3",
    SYSTEM_V8: "rank",
    kind_prompt.SYSTEM: "issue_kind",
}


def stage_of(request: dict[str, Any]) -> str:
    system = request["system"][0]["text"]
    if system in SYSTEM_STAGES:
        return SYSTEM_STAGES[system]
    assert system == confirm_prompt.SYSTEM, system[:60]
    user = request["messages"][0]["content"][0]["text"]
    if user.startswith("두 이슈가"):
        return "judge"
    if user.startswith("뉴스 타임라인 편집자로서"):
        return "check"
    return "plan_reader"


@dataclass
class PairScript:
    """(A, B) 쌍 하나에 정해 둔 답이다. None 이면 그 투표자는 DIFFERENT(통과 못 함)로 답한다."""

    screen: str = "SAME_STORY"
    verify: str | None = "SAME_STORY"
    matter: str | None = "specific_step"
    rank: str | None = "SAME_STORY"
    # rank 행이 check_affirm 을 통과하도록 쓰는 단어다. 두 이슈 제목에 모두 있어야 한다.
    rank_key: str = ""
    judge: tuple[str, str, str] | None = ("SAME_STORY", "SPECIFIC", "SPECIFIC")
    check: tuple[str, bool, bool] | None = ("NEW_STEP", True, True)
    # 계획 이행 확인 답(fit, a_quote, b_quote, a_actor, b_actor, shared)이다. None 이면 fit 을
    # NONE 으로 답한다.
    plan: tuple[str, str, str, str, str, str] | None = None


@dataclass
class FakeBedrock:
    """Converse 가짜다. pairs 는 {(부모 제목, 자식 제목): PairScript} 다."""

    pairs: dict[tuple[str, str], PairScript] = field(default_factory=dict)
    kinds: dict[str, str] = field(default_factory=dict)
    # (모델, 제목)별 성격이다. 있으면 kinds 보다 먼저 쓴다.
    model_kinds: dict[tuple[str, str], str] = field(default_factory=dict)
    # 성격 분류를 부른 모델별 횟수다.
    kind_models: Counter = field(default_factory=Counter)
    # 이 제목이 사용자 메시지에 있으면 Bedrock 장애처럼 예외를 던진다.
    fail_titles: set[str] = field(default_factory=set)
    # True 면 rank 응답을 읽을 수 없는 텍스트로 준다.
    garbled_rank: bool = False
    calls: Counter = field(default_factory=Counter)
    requests: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, request: dict[str, Any]) -> dict[str, Any]:
        stage = stage_of(request)
        self.calls[stage] += 1
        self.requests.append(request)
        user = request["messages"][0]["content"][0]["text"]
        for title in self.fail_titles:
            if title in user:
                raise RuntimeError(f"bedrock down for {title}")
        if stage == "issue_kind":
            title = re.search(r"^제목: (.*)$", user, re.M).group(1)
            model = request["modelId"]
            self.kind_models[model] += 1
            kind = self.model_kinds.get((model, title), self.kinds.get(title, "event"))
            return _tool({"main_news": title, "reason": f"{title} 판단", "kind": kind})
        if stage == "rank":
            return self._rank(user)
        if stage == "plan_extract":
            return _tool({"plans": []})
        script = self._script(stage, user)
        return self._answer(stage, script)

    # ── 쌍 찾기 ──
    def _script(self, stage: str, user: str) -> PairScript | None:
        a_text, b_text = _split(stage, user)
        for (parent, child), script in self.pairs.items():
            if _has_title(a_text, parent) and _has_title(b_text, child):
                return script
        return None

    def _answer(self, stage: str, s: PairScript | None) -> dict[str, Any]:
        if stage == "screen":
            label = s.screen if s else "DIFFERENT"
            return _text({"label": label})
        if stage == "verify.evidence":
            label = (s.verify if s else None) or "DIFFERENT"
            return _tool(
                {
                    "matter_in_a": "a",
                    "matter_in_b": "b",
                    "same_specific_matter": label != "DIFFERENT",
                    "label": label,
                    "reason": "r",
                }
            )
        if stage == "verify.matter":
            basis = (s.matter if s else None) or "none"
            label = "SAME_STORY" if basis != "none" else "DIFFERENT"
            return _tool(
                {
                    "a_matter": "a",
                    "matter_scope": "specific",
                    "b_own_news": "b",
                    "basis": basis,
                    "label": label,
                    "reason": "r",
                }
            )
        if stage in ("plan_step.planstep", "plan_step.evidence3"):
            return _tool({"label": "DIFFERENT"})
        if stage == "judge":
            label, a_scope, b_scope = (s.judge if s else None) or (
                "DIFFERENT",
                "SPECIFIC",
                "SPECIFIC",
            )
            return _text(
                {
                    "a_matter": "a",
                    "a_scope": a_scope,
                    "b_matter": "b",
                    "b_scope": b_scope,
                    "label": label,
                }
            )
        if stage == "check":
            role, a_spec, b_spec = (s.check if s else None) or ("OTHER_MATTER", False, False)
            return _text(
                {"a_specific": a_spec, "b_specific": b_spec, "new_development": "x", "b_role": role}
            )
        if stage == "plan_reader":
            if not s or not s.plan:
                return _text(
                    {
                        "a_quote": "",
                        "b_quote": "",
                        "a_actor": "",
                        "b_actor": "",
                        "fit": "NONE",
                        "shared": "",
                    }
                )
            fit, a_quote, b_quote, a_actor, b_actor, shared = s.plan
            return _text(
                {
                    "a_quote": a_quote,
                    "b_quote": b_quote,
                    "a_actor": a_actor,
                    "b_actor": b_actor,
                    "fit": fit,
                    "shared": shared,
                }
            )
        raise AssertionError(stage)

    def _rank(self, user: str) -> dict[str, Any]:
        if self.garbled_rank:
            return {
                "output": {"message": {"content": [{"text": "생각 중..."}]}},
                "stopReason": "end_turn",
            }
        new_part, _, rest = user.partition("EARLIER candidates")
        child = re.search(r"^Title: (.*)$", new_part, re.M).group(1)
        rows = []
        for k, block in enumerate(re.split(r"\n\nC\d+ ", rest)[1:], start=1):
            parent = re.search(r"^Title: (.*)$", block, re.M).group(1)
            script = self.pairs.get((parent, child))
            label = (script.rank if script else None) or "DIFFERENT"
            key = script.rank_key if script else ""
            rows.append(
                {
                    "c": k,
                    "new_matter": f"{key} 새 단계",
                    "old_matter": f"{key} 이전 단계",
                    "anchor": key or None,
                    "key": key or None,
                    "stage": "새 단계" if label == "SAME_STORY" else None,
                    "step_by": "party" if label == "SAME_STORY" else None,
                    "label": label,
                }
            )
        return _text({"candidates": rows, "parent": None, "relation": None})


def _split(stage: str, user: str) -> tuple[str, str]:
    """사용자 메시지에서 A 쪽과 B 쪽 텍스트를 나눠 꺼낸다."""

    if stage in ("judge", "check", "plan_reader"):
        rest = user.split("\n[A]\n", 1)[1]
        a, b = rest.split("\n\n[B]\n", 1)
        return a, b
    a, b = user.split("\n\nIssue B", 1)
    return a, b


def _has_title(text: str, title: str) -> bool:
    return re.search(rf"^(Title|제목): {re.escape(title)}$", text, re.M) is not None


def _tool(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "output": {"message": {"content": [{"toolUse": {"name": "t", "input": payload}}]}},
        "stopReason": "tool_use",
        "usage": {"inputTokens": 1000, "outputTokens": 100},
    }


def _text(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "output": {"message": {"content": [{"text": json.dumps(payload, ensure_ascii=False)}]}},
        "stopReason": "end_turn",
        "usage": {"inputTokens": 1000, "outputTokens": 100},
    }


class MemoryStore:
    """LLM 응답 캐시 가짜다. 같은 키에는 먼저 쓴 응답만 남는다."""

    def __init__(self) -> None:
        self.rows: dict[str, CallRecord] = {}

    def get(self, cache_key: str) -> dict[str, Any] | None:
        row = self.rows.get(cache_key)
        return None if row is None else row.response

    def put(self, record: CallRecord) -> dict[str, Any]:
        return self.rows.setdefault(record.cache_key, record).response
