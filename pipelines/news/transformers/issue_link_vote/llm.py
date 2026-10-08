"""이슈 연결 LLM 호출을 맡는다. 단계별 모델 라우팅, 응답 검증과 한 번의 형식 재요청, 입력 키 캐시,
호출 상한을 다룬다.

호출하는 곳은 단계(stage) 이름만 넘기고, 그 단계가 속한 역할의 모델(pipelines.common.config 의
chat_model(role))이 답한다.

    역할                    단계
    issue_link_proposer     screen, verify.evidence, verify.matter, plan_extract,
                            plan_step.planstep, plan_step.evidence3, rank
    issue_link_confirmer    judge, check, plan_reader
    issue_kind              issue_kind
    issue_kind_screen       issue_kind(1차 분류, call(model=...) 로 같은 단계를 다른 모델로 부른다)

어댑터(Bedrock Converse):
  - 도구 단계: 모델이 도구 강제 호출을 지원하면 원래 toolConfig 와 toolChoice 로 보낸다. 지원하지
    않으면(JSON 모드) 도구 스키마를 사용자 메시지에 붙이고 JSON 객체 하나로 답하게 한 뒤, 본문에서
    스키마를 통과하는 첫 객체를 고른다.
  - 자유 텍스트 단계: 원래 프롬프트(이미 JSON 객체 하나를 요구한다)를 보내고 단계 스키마로 검증한다.
  - reasoningContent 블록과 본문의 <think> 블록은 읽지 않는다.
  - 검증에 실패한 응답(객체 없음, 스키마 오류, 빈 출력, 잘림)은 짧은 재요청 메시지로 한 번 더
    묻는다. 그래도 실패하면 객체 없이 돌려준다. 투표자는 이를 가장 보수적인 답(연결 안 함, 통과
    못 함)으로 읽고, 성격 분류는 분류하지 못한 것으로 본다.

캐시: 키는 sha256(모델|단계|프롬프트 버전|호출 번호|요청 전체)이다. 요청 원문은 뉴스 텍스트를
담고 있어 저장하지 않고 키(해시)만 남긴다. 같은 입력이면 다음 실행(재판정 포함)도 저장된 응답을
그대로 쓴다. 두 실행이 같은 키를 동시에 쓰면 먼저 쓴 응답만 남고, 늦은 쪽도 그 응답을 쓴다.
재요청 뒤에도 읽지 못한 응답은 캐시에서 지워 다음 실행이 다시 묻게 한다. 같은 실행에서는 메모리에
남긴 답을 다시 쓴다. 모델이 거절한 요청(api_error)은 다시 물어도 같으므로 지우지 않는다.

상한: Bedrock 을 새로 부른 횟수만 세고 캐시 적중은 세지 않는다. 상한에 닿으면 예외를 던지고, job 은
이슈 상한이면 그 이슈만, 실행당 상한이면 남은 대상 전부를 다음 실행으로 미룬다. 이미 받은 응답은
캐시에 남아 다음 실행이 이어서 진행한다.
"""

from __future__ import annotations

import copy
import hashlib
import json
import random
import re
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol

from pipelines.news.transformers.issue_link_vote import schemas

PROPOSER = "issue_link_proposer"
CONFIRMER = "issue_link_confirmer"
ISSUE_KIND = "issue_kind"
# 성격 분류의 1차 모델 역할이다. 단계는 issue_kind 와 같고 모델만 다르므로 STAGES 에 넣지 않는다.
ISSUE_KIND_SCREEN = "issue_kind_screen"

STAGES: dict[str, str] = {
    "screen": PROPOSER,
    "verify.evidence": PROPOSER,
    "verify.matter": PROPOSER,
    "plan_extract": PROPOSER,
    "plan_step.planstep": PROPOSER,
    "plan_step.evidence3": PROPOSER,
    "rank": PROPOSER,
    "judge": CONFIRMER,
    "check": CONFIRMER,
    "plan_reader": CONFIRMER,
    "issue_kind": ISSUE_KIND,
}

# 캐시 키와 감사 기록에 남기는 프롬프트 버전이다. 프롬프트를 바꾸면 버전도 올려야 캐시에 남은 옛
# 응답을 다시 쓰지 않는다.
PROMPT_VERSIONS: dict[str, str] = {
    "screen": "pair-screen-v1",
    "verify.evidence": "pair-evidence-v2",
    "verify.matter": "pair-matter-v2",
    "plan_extract": "pair-plan-extract-v3",
    "plan_step.planstep": "pair-plan-step-v3",
    "plan_step.evidence3": "pair-step-evidence-v3",
    "rank": "rank-v8",
    "judge": "scope-v4",
    "check": "scopecheck-v2",
    "plan_reader": "plan-v5",
    "issue_kind": "issue-kind-v4",
}

REPAIR_TEXT = (
    "Your previous reply could not be used: {err}. Reply again with exactly one JSON object in the "
    "required format and nothing else: no code fences, no text before or after the object."
)
JSON_MODE_TEXT = (
    "\n\nNo tool can be called in this conversation. Instead of calling the {name} tool, "
    "reply with exactly one JSON object: the input you would pass to the tool. The object must "
    "follow this JSON schema:\n{schema}\nReply with the JSON object only: no code fences, no text "
    "before or after it."
)
RETRYABLE_API_ERRORS = {
    "ThrottlingException",
    "ServiceUnavailableException",
    "ModelNotReadyException",
    "InternalServerException",
    "ModelErrorException",
    "ServiceQuotaExceededException",
}
MAX_API_RETRIES = 8
MAX_CONNECTION_RETRIES = 4

# 비용을 추정할 때 쓰는 단가다(USD / 100만 토큰, 입력·출력). us.* 지역 추론 프로필은 Anthropic
# 기준가의 1.1배다. 표에 없는 모델은 0 으로 세고 로그에 남긴다.
PRICES: dict[str, tuple[float, float]] = {
    "us.anthropic.claude-sonnet-4-6": (3.30, 16.50),
    "global.anthropic.claude-sonnet-4-6": (3.00, 15.00),
    "us.anthropic.claude-haiku-4-5-20251001-v1:0": (1.10, 5.50),
    "global.anthropic.claude-haiku-4-5-20251001-v1:0": (1.00, 5.00),
    "moonshotai.kimi-k2.5": (0.60, 3.00),
}


@dataclass(frozen=True)
class Caps:
    """모델마다 Converse 에서 지원하는 기능이다. tool_choice 가 False 면 도구 단계를 JSON 모드로
    보낸다."""

    tool_choice: bool = True
    temperature: bool = True
    system: bool = True
    max_tokens_floor: int = 0


# 표에 없는 모델은 기본값(도구 강제 호출·온도·시스템 프롬프트 모두 지원)을 쓴다. 기본 모델인
# Kimi K2.5 와 Claude 는 Bedrock Converse 에서 셋 다 지원하므로 표가 비어 있다.
MODEL_CAPS: dict[str, Caps] = {}


def caps_for(model: str) -> Caps:
    return MODEL_CAPS.get(model, Caps())


class BudgetExceeded(RuntimeError):
    """새 호출 상한에 닿았을 때 던진다. 이미 받은 응답은 캐시에 남는다."""


class RunBudgetExceeded(BudgetExceeded):
    pass


class IssueBudgetExceeded(BudgetExceeded):
    pass


@dataclass(frozen=True)
class CallRecord:
    """캐시·감사 기록 한 건이다. 요청 원문은 담지 않고, cache_key 가 요청을 포함한 입력의 해시다."""

    cache_key: str
    stage: str
    model: str
    prompt_version: str
    sample: int
    cluster_id: int
    candidate_cluster_id: int | None
    response: dict[str, Any]
    input_tokens: int
    output_tokens: int
    latency_ms: int | None


class CallStore(Protocol):
    """응답 캐시 저장소다. 운영 구현은 repositories/postgres/issue_links.py 에 있고, 테스트는 메모리
    가짜를 넣는다."""

    def get(self, cache_key: str) -> dict[str, Any] | None: ...

    def put(self, record: CallRecord) -> dict[str, Any]: ...

    def delete(self, cache_key: str) -> None: ...


Invoker = Callable[[dict[str, Any]], dict[str, Any]]


# ── JSON 추출과 검증 ────────────────────────────────────────────────────────

_THINK = re.compile(r"<(think|thinking|reasoning)>.*?</\1>", re.S | re.I)
_WRAPPERS = ("input", "arguments", "parameters", "args", "tool_input")


def json_objects(text: str) -> list[dict]:
    """본문에 있는 최상위 JSON 객체를 순서대로 모두 꺼낸다. 본문 안의 추론 블록은 먼저 지운다."""

    t = _THINK.sub("", text or "")
    for tag in ("</think>", "</thinking>", "</reasoning>"):
        if tag in t:
            t = t.rsplit(tag, 1)[1]
    dec = json.JSONDecoder()
    out: list[dict] = []
    i = t.find("{")
    while i != -1:
        try:
            obj, end = dec.raw_decode(t, i)
        except json.JSONDecodeError:
            i = t.find("{", i + 1)
            continue
        if isinstance(obj, dict):
            out.append(obj)
        i = t.find("{", end)
    return out


def pick(
    objects: list[dict], schema: dict, tool_name: str | None = None
) -> tuple[dict | None, str]:
    """스키마를 통과하는 첫 객체를 고르며, 도구 호출 래퍼 안의 객체도 본다. 통과하는 객체가 없으면
    처음 만난 검증 오류를 돌려준다."""

    first_err = "no JSON object in the reply"
    for obj in objects:
        tries = [obj]
        for k in (*_WRAPPERS, tool_name):
            if k and isinstance(obj.get(k), dict):
                tries.append(obj[k])
        if len(obj) == 1 and isinstance(next(iter(obj.values())), dict):
            tries.append(next(iter(obj.values())))
        for cand in tries:
            ok, value, err = schemas.validate(cand, schema)
            if ok:
                return value, ""
            if first_err == "no JSON object in the reply":
                first_err = err
    return None, first_err


def response_parts(resp: dict[str, Any]) -> dict[str, Any]:
    """Converse 응답에서 본문(추론 블록 제외), 도구 입력, 종료 사유, 사용량을 꺼낸다."""

    blocks = [
        b
        for b in resp.get("output", {}).get("message", {}).get("content", [])
        if isinstance(b, dict)
    ]
    text = "".join(b["text"] for b in blocks if "text" in b and "reasoningContent" not in b)
    tool_input = next((b["toolUse"].get("input") for b in blocks if "toolUse" in b), None)
    usage = resp.get("usage") or {}
    return {
        "text": text,
        "tool_input": tool_input,
        "stop_reason": resp.get("stopReason"),
        "usage": {
            "inputTokens": int(usage.get("inputTokens", 0) or 0),
            "outputTokens": int(usage.get("outputTokens", 0) or 0),
        },
    }


# ── 응답 ────────────────────────────────────────────────────────────────────


@dataclass
class Reply:
    stage: str
    model: str
    key: str
    obj: dict | None
    error: str
    text: str
    stop_reason: str | None
    usage: dict[str, int]
    retries: int
    fresh_calls: int
    adapter: str

    @property
    def parse_ok(self) -> bool:
        return self.obj is not None

    def parser_text(self) -> str:
        """자유 텍스트 단계의 파서에 넘길 텍스트다. 객체가 없으면 "" 를 돌려줘 파서가 보수적인 답을
        고르게 한다."""

        return json.dumps(self.obj, ensure_ascii=False) if self.obj is not None else ""

    def tool_row(self) -> dict[str, Any]:
        """도구 단계(pair.py)의 파서가 읽는 응답 행이다."""

        return {
            "model": self.model,
            "text": self.text,
            "usage": self.usage,
            "stop_reason": "tool_use" if self.obj is not None else (self.stop_reason or "invalid"),
            "tool_input": self.obj,
        }


@dataclass
class CallStats:
    calls: int = 0
    cache_hits: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    parse_failures: int = 0
    # 읽지 못해 캐시에서 지운 응답 수다.
    evicted: int = 0
    by_stage: dict[str, int] = field(default_factory=dict)


def _sum_usage(attempts: list[dict[str, Any]]) -> dict[str, int]:
    total = {"inputTokens": 0, "outputTokens": 0}
    for a in attempts:
        u = a.get("usage") or {}
        total["inputTokens"] += int(u.get("inputTokens", 0) or 0)
        total["outputTokens"] += int(u.get("outputTokens", 0) or 0)
    return total


def parallel_map(fn: Callable[[Any], Any], items: Iterable[Any], workers: int) -> list[Any]:
    """fn 을 스레드로 나눠 부르고 입력 순서대로 결과를 돌려준다. 한 건이 예외를 던지면 그 예외를
    그대로 던진다."""

    items = list(items)
    if len(items) <= 1 or workers <= 1:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fn, items))


def cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    pin, pout = PRICES.get(model, (0.0, 0.0))
    return input_tokens / 1e6 * pin + output_tokens / 1e6 * pout


# ── 클라이언트 ──────────────────────────────────────────────────────────────


class LlmClient:
    """단계별 모델로 Converse 를 부르고 응답을 캐시한다. 스레드 안전하다.

    models 는 {단계: 모델 ID} 이며 job 이 설정에서 채운다. invoke 는 Converse 요청 dict 를 받아 응답
    dict 를 돌려준다. 운영에서는 boto3 bedrock-runtime 의 converse 를, 테스트에서는 가짜를 넣는다.
    """

    def __init__(
        self,
        models: dict[str, str],
        store: CallStore,
        invoke: Invoker,
        *,
        run_cap: int,
        issue_cap: int,
        retry: bool = True,
        sleep: Callable[[float], None] = time.sleep,
    ):
        missing = sorted(set(STAGES) - set(models))
        if missing:
            raise ValueError(f"모델이 정해지지 않은 단계: {missing}")
        self.models = dict(models)
        self.store = store
        self.invoke = invoke
        self.run_cap = run_cap
        self.issue_cap = issue_cap
        self.retry = retry
        self.sleep = sleep
        self.lock = threading.Lock()
        self.key_locks: dict[str, threading.Lock] = {}
        self.replies: dict[str, Reply] = {}
        self.stats = CallStats()
        self.issue_calls = 0
        self.unpriced: set[str] = set()

    # ── 상한 ──
    def begin_issue(self) -> None:
        with self.lock:
            self.issue_calls = 0

    def run_budget_left(self) -> bool:
        with self.lock:
            return self.stats.calls < self.run_cap

    def _reserve(self, stage: str) -> None:
        with self.lock:
            if self.stats.calls >= self.run_cap:
                raise RunBudgetExceeded(f"런 호출 상한 {self.run_cap} 도달({stage})")
            if self.issue_calls >= self.issue_cap:
                raise IssueBudgetExceeded(f"이슈 호출 상한 {self.issue_cap} 도달({stage})")
            self.stats.calls += 1
            self.issue_calls += 1
            self.stats.by_stage[stage] = self.stats.by_stage.get(stage, 0) + 1

    # ── 요청 ──
    def model_for(self, stage: str) -> str:
        return self.models[stage]

    @staticmethod
    def wire(
        model: str,
        system: str,
        user: str,
        max_tokens: int,
        temperature: float,
        tool: dict | None,
    ) -> tuple[dict[str, Any], str]:
        caps = caps_for(model)
        text = user
        adapter = "free"
        if tool is not None:
            if caps.tool_choice:
                adapter = "tool"
            else:
                adapter = "json"
                schema = json.dumps(tool["schema"], ensure_ascii=False, indent=1)
                text = user + JSON_MODE_TEXT.replace("{name}", tool["name"]).replace(
                    "{schema}", schema
                )
        if not caps.system:
            text = f"{system}\n\n{text}"
        inference: dict[str, Any] = {"maxTokens": max(int(max_tokens), caps.max_tokens_floor)}
        if caps.temperature:
            inference["temperature"] = temperature
        req: dict[str, Any] = {"modelId": model}
        if caps.system:
            req["system"] = [{"text": system}]
        req["messages"] = [{"role": "user", "content": [{"text": text}]}]
        req["inferenceConfig"] = inference
        if adapter == "tool":
            req["toolConfig"] = {
                "tools": [
                    {
                        "toolSpec": {
                            "name": tool["name"],
                            "description": tool["description"],
                            "inputSchema": {"json": tool["schema"]},
                        }
                    }
                ],
                "toolChoice": {"tool": {"name": tool["name"]}},
            }
        return req, adapter

    @staticmethod
    def key(model: str, stage: str, sample: int, req: dict[str, Any]) -> str:
        version = PROMPT_VERSIONS[stage.removesuffix(".repair")]
        body = json.dumps(req, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(f"{model}|{stage}|{version}|{sample}|{body}".encode()).hexdigest()

    @staticmethod
    def repair_wire(req: dict[str, Any], adapter: str, prev_text: str, err: str) -> dict[str, Any]:
        rep = copy.deepcopy(req)
        note = REPAIR_TEXT.replace("{err}", err[:300])
        prev = (prev_text or "").strip()
        if adapter == "tool" or not prev:
            rep["messages"][0]["content"][0]["text"] += "\n\n" + note
        else:
            rep["messages"] = [
                rep["messages"][0],
                {"role": "assistant", "content": [{"text": prev[-3000:]}]},
                {"role": "user", "content": [{"text": note}]},
            ]
        return rep

    # ── Bedrock ──
    def _invoke(self, req: dict[str, Any]) -> tuple[dict[str, Any], int]:
        """(응답 부분, 지연 ms) 를 돌려준다. 일시 오류는 대기 시간을 늘려 가며 다시 부른다. 모델이
        거절한 요청(너무 긴 입력 등)은 빈 응답으로 돌려 보수적인 답이 되게 하고, 그 밖의 오류는
        그대로 던진다."""

        from botocore.exceptions import ClientError, ReadTimeoutError
        from botocore.exceptions import ConnectionError as BotoConnectionError

        api_retries = 0
        while True:
            t0 = time.monotonic()
            try:
                resp = self.invoke(req)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in RETRYABLE_API_ERRORS and api_retries < MAX_API_RETRIES:
                    api_retries += 1
                    self.sleep(min(60.0, 2.0**api_retries) * (0.5 + random.random()))
                    continue
                if code == "ValidationException" and "model identifier is invalid" not in str(e):
                    latency = round((time.monotonic() - t0) * 1000)
                    return {
                        "text": "",
                        "tool_input": None,
                        "stop_reason": "api_error",
                        "usage": {"inputTokens": 0, "outputTokens": 0},
                        "api_error": f"{code}: {str(e)[:300]}",
                    }, latency
                raise RuntimeError(f"Bedrock {code} ({req['modelId']}): {e}") from None
            except (ReadTimeoutError, BotoConnectionError):
                if api_retries < MAX_CONNECTION_RETRIES:
                    api_retries += 1
                    self.sleep(2.0**api_retries)
                    continue
                raise
            return response_parts(resp), round((time.monotonic() - t0) * 1000)

    def _attempt(
        self,
        key: str,
        model: str,
        stage: str,
        sample: int,
        req: dict[str, Any],
        cluster_id: int,
        candidate_id: int | None,
    ) -> tuple[dict[str, Any], bool]:
        """(응답, 새로 불렀는지) 를 돌려준다. 캐시에 없을 때만 상한을 확인하고 Bedrock 을 부른다."""

        cached = self.store.get(key)
        if cached is not None:
            with self.lock:
                self.stats.cache_hits += 1
            return cached, False

        self._reserve(stage)
        response, latency = self._invoke(req)
        usage = response.get("usage") or {}
        record = CallRecord(
            cache_key=key,
            stage=stage,
            model=model,
            prompt_version=PROMPT_VERSIONS[stage.removesuffix(".repair")],
            sample=sample,
            cluster_id=cluster_id,
            candidate_cluster_id=candidate_id,
            response=response,
            input_tokens=int(usage.get("inputTokens", 0) or 0),
            output_tokens=int(usage.get("outputTokens", 0) or 0),
            latency_ms=latency,
        )
        with self.lock:
            self.stats.input_tokens += record.input_tokens
            self.stats.output_tokens += record.output_tokens
            self.stats.cost_usd += cost_usd(model, record.input_tokens, record.output_tokens)
            if model not in PRICES:
                self.unpriced.add(model)
        return self.store.put(record), True

    @staticmethod
    def _evaluate(
        resp: dict[str, Any], adapter: str, schema: dict, tool_name: str | None
    ) -> tuple[dict | None, str]:
        if adapter == "tool" and isinstance(resp.get("tool_input"), dict):
            ok, value, err = schemas.validate(resp["tool_input"], schema)
            if ok:
                return value, ""
            obj, _ = pick(json_objects(resp.get("text") or ""), schema, tool_name)
            return (obj, "") if obj is not None else (None, f"tool input invalid: {err}")
        text = resp.get("text") or ""
        if resp.get("api_error"):
            return None, f"request refused: {resp['api_error'][:200]}"
        if not text.strip():
            if resp.get("stop_reason") == "max_tokens":
                return None, "output truncated before any text"
            return None, "empty output"
        obj, err = pick(json_objects(text), schema, tool_name)
        if obj is None and resp.get("stop_reason") == "max_tokens":
            err = f"output truncated ({err})"
        return obj, err

    def _evict(self, keys: list[str], attempts: list[dict[str, Any]]) -> None:
        """읽지 못한 응답을 캐시에서 지운다. 모델이 거절한 응답은 남긴다."""

        for key, response in zip(keys, attempts, strict=True):
            if response.get("api_error"):
                continue
            self.store.delete(key)
            with self.lock:
                self.stats.evicted += 1

    # ── 호출 ──
    def call(
        self,
        stage: str,
        *,
        system: str,
        user: str,
        max_tokens: int,
        cluster_id: int,
        candidate_id: int | None = None,
        temperature: float = 0.0,
        sample: int = 0,
        tool: dict | None = None,
        model: str | None = None,
    ) -> Reply:
        """model 을 주면 단계의 역할 모델 대신 그 모델이 답한다. 캐시 키에 모델이 들어가므로 같은
        단계라도 모델마다 응답을 따로 남긴다."""

        model = model or self.model_for(stage)
        req, adapter = self.wire(model, system, user, max_tokens, temperature, tool)
        key = self.key(model, stage, sample, req)
        with self.lock:
            if key in self.replies:
                return self.replies[key]
            klock = self.key_locks.setdefault(key, threading.Lock())
        with klock:
            with self.lock:
                if key in self.replies:
                    return self.replies[key]
            schema = tool["schema"] if tool is not None else schemas.STAGE_SCHEMAS[stage]
            tool_name = tool["name"] if tool is not None else None
            first, fresh = self._attempt(key, model, stage, sample, req, cluster_id, candidate_id)
            keys, attempts, new = [key], [first], int(fresh)
            obj, err = self._evaluate(first, adapter, schema, tool_name)
            retries = 0
            if obj is None and self.retry:
                rstage = f"{stage}.repair"
                rreq = self.repair_wire(req, adapter, first.get("text") or "", err)
                rkey = self.key(model, rstage, sample, rreq)
                second, fresh = self._attempt(
                    rkey, model, rstage, sample, rreq, cluster_id, candidate_id
                )
                keys.append(rkey)
                attempts.append(second)
                new += int(fresh)
                retries = 1
                obj, err2 = self._evaluate(second, adapter, schema, tool_name)
                err = "" if obj is not None else f"{err}; after repair: {err2}"
            if obj is None:
                self._evict(keys, attempts)
            last = attempts[-1]
            reply = Reply(
                stage=stage,
                model=model,
                key=key,
                obj=obj,
                error=err,
                text=last.get("text") or "",
                stop_reason=last.get("stop_reason"),
                usage=_sum_usage(attempts),
                retries=retries,
                fresh_calls=new,
                adapter=adapter,
            )
            with self.lock:
                self.replies[key] = reply
                if obj is None:
                    self.stats.parse_failures += 1
            return reply
