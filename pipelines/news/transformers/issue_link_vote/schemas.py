"""LLM 응답 스키마와 작은 JSON 스키마 검증기다.

도구 단계(verify.*, plan_extract, plan_step.*, issue_kind)는 각자 보내는 도구 스키마로 검증하고,
자유 텍스트 단계(screen, rank, judge, check, plan_reader)는 아래 스키마로 검증한다. 스키마는 각
단계의 파서와 코드 점검이 읽는 필드만 요구한다.

검증기는 type(여러 타입 포함), enum, required, properties, items 만 본다. maxItems 는 보지 않으므로
도구가 돌려준 항목을 모두 쓴다. 모든 모델에 똑같이 적용하는 형식 정규화는 다음과 같다:
  - enum 문자열은 대소문자를 무시하고 맞춘 뒤 스키마 표기로 바꾼다.
  - "x-soft-enum" 은 대소문자만 맞춰 바꾸고, 목록에 없는 값이어도 실패시키지 않는다. step_by 처럼
    목록 밖의 값을 파서가 이미 "통과 못 함"으로 읽는 보조 필드에 쓴다.
  - 정수는 정수인 실수나 숫자 문자열로, 불리언은 "true"/"false" 문자열로 와도 받는다. 문자열 자리에
    숫자가 와도 받는다(이름이 숫자인 대상이 있다).
"""

from __future__ import annotations

from typing import Any

LABELS = ["SAME_EVENT", "SAME_STORY", "DIFFERENT"]
STR = {"type": "string"}
OPT_STR = {"type": ["string", "null"]}

SCREEN = {
    "type": "object",
    "properties": {
        "a_subject": STR,
        "b_subject": STR,
        "shared_subject": STR,
        "label": {"type": "string", "enum": LABELS},
        "reason": STR,
    },
    "required": ["label"],
}

JUDGE = {
    "type": "object",
    "properties": {
        "a_matter": STR,
        "a_scope": {"type": "string", "enum": ["SPECIFIC", "THEME"]},
        "b_matter": STR,
        "b_scope": {"type": "string", "enum": ["SPECIFIC", "THEME"]},
        "same_matter": {"type": "boolean"},
        "label": {"type": "string", "enum": LABELS},
    },
    "required": ["a_scope", "b_scope", "label"],
}

CHECK = {
    "type": "object",
    "properties": {
        "a_specific": {"type": "boolean"},
        "b_specific": {"type": "boolean"},
        "new_development": STR,
        "b_role": {
            "type": "string",
            "enum": [
                "SAME_REPORT",
                "NEW_STEP",
                "NEXT_INSTALLMENT",
                "THEME_ONLY",
                "COMMENTARY",
                "OTHER_MATTER",
            ],
        },
    },
    "required": ["a_specific", "b_specific", "b_role"],
}

PLAN_READER = {
    "type": "object",
    "properties": {
        "b_done": OPT_STR,
        "b_quote": OPT_STR,
        "a_pending": OPT_STR,
        "a_quote": OPT_STR,
        "a_actor": OPT_STR,
        "b_actor": OPT_STR,
        "fit": {"type": "string", "enum": ["THE_ITEM", "ONE_OF_MANY", "OTHER", "NONE"]},
        "shared": OPT_STR,
    },
    "required": ["a_quote", "b_quote", "a_actor", "b_actor", "fit", "shared"],
}

RANK = {
    "type": "object",
    "properties": {
        "candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "c": {"type": ["integer", "string"]},
                    "new_matter": OPT_STR,
                    "old_matter": OPT_STR,
                    "anchor": OPT_STR,
                    "key": OPT_STR,
                    "stage": OPT_STR,
                    "step_by": {
                        "type": ["string", "null"],
                        "x-soft-enum": ["party", "authority", "schedule", "other"],
                    },
                    "label": {"type": "string", "enum": LABELS},
                },
                "required": ["c", "label"],
            },
        },
        "parent": {"type": ["integer", "string", "null"]},
        "relation": {"type": ["string", "null"]},
    },
    "required": ["candidates"],
}

STAGE_SCHEMAS = {
    "screen": SCREEN,
    "judge": JUDGE,
    "check": CHECK,
    "plan_reader": PLAN_READER,
    "rank": RANK,
}


def _types(schema: dict) -> list[str]:
    t = schema.get("type")
    if t is None:
        return []
    return [t] if isinstance(t, str) else list(t)


def _coerce(value: Any, types: list[str]) -> tuple[bool, Any]:
    if not types:
        return True, value
    if value is None:
        return ("null" in types), value
    if isinstance(value, bool):
        if "boolean" in types:
            return True, value
        if "string" in types:
            return True, str(value).lower()
        return False, value
    if isinstance(value, int):
        if "integer" in types or "number" in types:
            return True, value
        if "string" in types:
            return True, str(value)
        return False, value
    if isinstance(value, float):
        if "number" in types:
            return True, value
        if "integer" in types and value.is_integer():
            return True, int(value)
        if "string" in types:
            return True, str(value)
        return False, value
    if isinstance(value, str):
        if "string" in types:
            return True, value
        s = value.strip()
        if "integer" in types and s.lstrip("-").isdigit():
            return True, int(s)
        if "boolean" in types and s.lower() in ("true", "false"):
            return True, s.lower() == "true"
        if "null" in types and s.lower() in ("null", "none", ""):
            return True, None
        return False, value
    if isinstance(value, dict):
        return ("object" in types), value
    if isinstance(value, list):
        return ("array" in types), value
    return False, value


def validate(value: Any, schema: dict, path: str = "$") -> tuple[bool, Any, str]:
    """(통과 여부, 정규화한 값, 오류 메시지) 를 돌려준다."""

    types = _types(schema)
    if not types and "properties" in schema:
        types = ["object"]
    ok, value = _coerce(value, types)
    if not ok:
        return False, value, f"{path}: expected {'/'.join(types)}, got {type(value).__name__}"
    if isinstance(value, str):
        for key, hard in (("enum", True), ("x-soft-enum", False)):
            allowed = [a for a in schema.get(key, []) if isinstance(a, str)]
            if not allowed:
                continue
            match = next((a for a in allowed if a.lower() == value.strip().lower()), None)
            if match is not None:
                value = match
            elif hard:
                return False, value, f"{path}: {value[:40]!r} is not one of {allowed}"
    elif "enum" in schema and value not in schema["enum"]:
        return False, value, f"{path}: {value!r} is not one of {schema['enum']}"
    if isinstance(value, dict):
        out = dict(value)
        for req in schema.get("required", []):
            if req not in out:
                return False, value, f"{path}: missing field {req!r}"
        for k, sub in (schema.get("properties") or {}).items():
            if k in out:
                ok, v, err = validate(out[k], sub, f"{path}.{k}")
                if not ok:
                    return False, value, err
                out[k] = v
        return True, out, ""
    if isinstance(value, list):
        items = schema.get("items")
        out_list = []
        for k, item in enumerate(value):
            if items:
                ok, v, err = validate(item, items, f"{path}[{k}]")
                if not ok:
                    return False, value, err
                item = v
            out_list.append(item)
        return True, out_list, ""
    return True, value, ""
