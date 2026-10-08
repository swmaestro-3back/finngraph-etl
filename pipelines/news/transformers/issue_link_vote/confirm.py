"""확인자인 judge 와 check(scope 점검)를 맡는다. 제안자(pair 나 rank)가 받아들인 쌍에만 묻는다.
judge 표는 scope 판정으로 정하고, scope 판정이 실패하면 계획 이행 확인 결과로 정한다.

judge   scope-v4(JUDGE_PROMPT, 판정 예시 포함). 각 이슈의 사안과 범위(SPECIFIC/THEME)를 적고 라벨을
        고른다. SAME_EVENT 이거나, SAME_STORY 이면서 두 범위가 모두 SPECIFIC 이면 통과한다.
plan    scope 판정이 통과하지 못했을 때만 묻는 계획 이행 확인(plan-v5). B 가 A 에 적힌 '아직
        이루어지지 않은 일'을 실행했는지 묻는다. 답이 THE_ITEM 이고 아래 코드 점검을 모두 통과해야
        한다.
          - 두 인용문이 각자의 이슈 텍스트에 있다. 공백·문장부호는 빼고 비교하며, 말줄임표로 줄인
            인용은 조각마다 확인한다.
          - 공통 대상의 단어(기업 이름 제외) 하나가 A 의 인용문에 있고, B 의 인용문이나 B 의 제목
            텍스트(이슈 제목·한 줄 요약·기사 제목이며 요약 문단은 제외)에도 있다.
          - 두 주체가 단어를 공유한다.
        통과하면 judge 표는 SAME_STORY 로 통과한다.
check   scopecheck-v2(CHECK_PROMPT). SAME_REPORT 이거나, NEW_STEP·NEXT_INSTALLMENT 이면서 두 쪽이
        모두 구체적 사안이면 통과한다.

이슈 요약문: B 의 요약문에는 기사를 모두 넣고, A 의 요약문에는 B 의 마지막 시각까지 나온 기사만
넣는다(기간도 그 시각에서 끊는다). 세 호출 모두 같은 시스템 프롬프트를 쓰고 온도는 0 이다.

판정 예시: 판정할 쌍과 주요 기업이 겹치는 예시는 뺀다. 예시마다 고정해 둔 주요 기업
이름(prompts/issue_link_confirm.py)을 판정할 쌍의 주요 기업 이름(companies.name)과 비교하며,
예시 기업을 DB 에서 찾지 않는다.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pipelines.news.transformers.issue_link_vote.issue import (
    CompanyName,
    Issue,
    collapse,
    company_aliases,
    fmt_minutes,
    issue_company_ids,
    last_seen,
)
from pipelines.news.transformers.issue_link_vote.llm import LlmClient
from pipelines.news.transformers.prompts.issue_link_confirm import (
    CHECK_PROMPT,
    JUDGE_EXAMPLES,
    JUDGE_PROMPT,
    PLAN_EXAMPLES,
    PLAN_READER_PROMPT,
    SYSTEM,
)

JUDGE_MAX_TOKENS = 500
CHECK_MAX_TOKENS = 500
PLAN_MAX_TOKENS = 700
MAX_MEMBER_TITLES = 8
LABELS = ("SAME_EVENT", "SAME_STORY", "DIFFERENT")
ROLES = ("SAME_REPORT", "NEW_STEP", "NEXT_INSTALLMENT", "THEME_ONLY", "COMMENTARY", "OTHER_MATTER")
PASS_ROLES = frozenset({"SAME_REPORT", "NEW_STEP", "NEXT_INSTALLMENT"})
FITS = ("THE_ITEM", "ONE_OF_MANY", "OTHER", "NONE")


# ── 이슈 요약문과 프롬프트 ──────────────────────────────────────────────────


def issue_block(issue: Issue, names: dict[int, CompanyName], cutoff: datetime | None = None) -> str:
    """이슈 한 건의 이슈 요약문을 만든다. cutoff 를 주면 그때까지 나온 기사만 적고 기간도
    거기서 끊는다."""

    members = []
    seen = set()
    for m in sorted(issue.members, key=lambda m: (m.published_at, collapse(m.title))):
        title = collapse(m.title)
        if not title or title in seen or (cutoff is not None and m.published_at > cutoff):
            continue
        seen.add(title)
        members.append(f"- {fmt_minutes(m.published_at)} {title}")
    members = members[:MAX_MEMBER_TITLES]
    last = issue.last_published_at
    if cutoff is not None and last > cutoff:
        last = cutoff
    primary = [names[c].name for c in issue.primary_company_ids if c in names]
    return "\n".join(
        [
            f"제목: {collapse(issue.title)}",
            f"기간: {fmt_minutes(issue.first_published_at)} ~ {fmt_minutes(last)}",
            f"주요 기업: {', '.join(primary) or '(없음)'}",
            f"한 줄 요약: {collapse(issue.node_summary) or '(없음)'}",
            f"요약: {collapse(issue.summary) or '(없음)'}",
            "기사 제목:",
            *(members or ["- (없음)"]),
        ]
    )


def blocks(a: Issue, b: Issue, names: dict[int, CompanyName]) -> tuple[str, str]:
    return issue_block(a, names, cutoff=last_seen(b)), issue_block(b, names)


def pair_company_names(a: Issue, b: Issue, names: dict[int, CompanyName]) -> set[str]:
    """판정할 쌍의 주요 기업 이름(companies.name)을 모은다."""

    ids = set(a.primary_company_ids) | set(b.primary_company_ids)
    return {collapse(names[c].name) for c in ids if c in names and collapse(names[c].name)}


def select_examples(examples: list[tuple[tuple[str, ...], str]], pair_names: set[str]) -> list[str]:
    """판정할 쌍과 주요 기업이 겹치지 않는 예시만 고른다. 기업이 없는 예시는 항상 넣는다."""

    return [
        text for companies, text in examples if not ({collapse(c) for c in companies} & pair_names)
    ]


def _with_examples(template: str, examples: list[str]) -> str:
    ex = "\n".join(f"- {e}" for e in examples) or "- (없음)"
    return template.replace("{examples}", ex)


def judge_prompt(a: Issue, b: Issue, names: dict[int, CompanyName]) -> str:
    block_a, block_b = blocks(a, b, names)
    examples = select_examples(JUDGE_EXAMPLES, pair_company_names(a, b, names))
    return _with_examples(JUDGE_PROMPT, examples).replace("{a}", block_a).replace("{b}", block_b)


def check_prompt(a: Issue, b: Issue, names: dict[int, CompanyName]) -> str:
    block_a, block_b = blocks(a, b, names)
    return CHECK_PROMPT.replace("{a}", block_a).replace("{b}", block_b)


def plan_prompt(a: Issue, b: Issue, names: dict[int, CompanyName]) -> str:
    block_a, block_b = blocks(a, b, names)
    examples = select_examples(PLAN_EXAMPLES, pair_company_names(a, b, names))
    return (
        _with_examples(PLAN_READER_PROMPT, examples).replace("{a}", block_a).replace("{b}", block_b)
    )


# ── 해석 ────────────────────────────────────────────────────────────────────


def _json(text: str) -> dict:
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        try:
            data = json.loads(text[start : end + 1])
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def parse_judge(text: str) -> dict[str, Any]:
    data = _json(text)
    label = str(data.get("label") or "").strip().upper()
    if label not in LABELS:
        label = "DIFFERENT"
    scope = {k: str(data.get(k) or "").strip().upper() for k in ("a_scope", "b_scope")}
    return {
        "label": label,
        **scope,
        "a_matter": collapse(str(data.get("a_matter") or "")),
        "b_matter": collapse(str(data.get("b_matter") or "")),
    }


def parse_check(text: str) -> dict[str, Any]:
    data = _json(text)
    role = str(data.get("b_role") or "").strip().upper()
    if role not in ROLES:
        role = "OTHER_MATTER"
    return {
        "role": role,
        "pass": role in PASS_ROLES,
        "a_specific": bool(data.get("a_specific")),
        "b_specific": bool(data.get("b_specific")),
        "new_development": collapse(str(data.get("new_development") or "")),
    }


def judge_passes(v: dict[str, Any]) -> bool:
    if v["label"] == "SAME_EVENT":
        return True
    if v["label"] != "SAME_STORY":
        return False
    return v["a_scope"] == "SPECIFIC" and v["b_scope"] == "SPECIFIC"


def check_passes(c: dict[str, Any]) -> bool:
    if c["role"] == "SAME_REPORT":
        return True
    if c["role"] not in PASS_ROLES:
        return False
    return c["a_specific"] and c["b_specific"]


# ── 계획 이행 확인 코드 점검 ────────────────────────────────────────────────

_KEEP = re.compile(r"[0-9A-Za-z가-힣]+")
_ELLIPSIS = re.compile(r"\.\.\.|…|⋯")


def norm(text: str) -> str:
    """영문자·숫자·한글만 남기고 소문자로 바꾼다. 공백과 문장부호는 일치 판단에서 뺀다."""

    return "".join(_KEEP.findall(text or "")).lower()


def grounded(quote: str, block: str) -> bool:
    """인용문이 이슈 요약문에 있는지 본다. 말줄임표로 줄인 인용은 조각(2글자 이상)이 모두
    있어야 한다."""

    q, nb = norm(quote), norm(block)
    if len(q) < 4:
        return False
    if q in nb:
        return True
    parts = [norm(p) for p in _ELLIPSIS.split(quote)]
    parts = [p for p in parts if p]
    return len(parts) > 1 and all(len(p) >= 2 and p in nb for p in parts)


def headlines(block: str) -> str:
    """이슈 요약문에서 제목 텍스트(이슈 제목, 한 줄 요약, 기사 제목 줄)만 남긴다. 요약 문단은 앞선
    사안을 배경으로 되풀이하는 일이 많아 뺀다."""

    keep = [ln for ln in block.splitlines() if ln.startswith(("제목:", "한 줄 요약:", "- "))]
    return "\n".join(keep)


def shared_object(shared: str, qa: str, qb: str, aliases: list[str]) -> bool:
    """공통 대상의 단어(기업 이름 제외) 하나가 A 의 인용문과 B 쪽 텍스트에 모두 있는지 본다."""

    na, nb = norm(qa), norm(qb)
    names = [norm(a) for a in aliases if norm(a)]
    for word in collapse(shared).split():
        w = norm(word)
        for n in names:
            w = w.replace(n, "")
        if len(w) >= 2 and w in na and w in nb:
            return True
    return False


def same_actor(a_actor: str, b_actor: str) -> bool:
    """한쪽 주체의 단어(2글자 이상)가 다른 쪽 주체에 있으면 같은 주체로 본다."""

    na, nb = norm(a_actor), norm(b_actor)
    if not na or not nb:
        return False
    for mine, other in ((a_actor, nb), (b_actor, na)):
        for word in re.split(r"[\s,·/()]+", mine):
            w = norm(word)
            if len(w) >= 2 and w in other:
                return True
    return False


def parse_plan(text: str) -> dict[str, Any]:
    data = _json(text)
    fit = str(data.get("fit") or "").strip().upper()
    if fit not in FITS:
        fit = "NONE"
    keys = ("a_pending", "a_quote", "b_done", "b_quote", "a_actor", "b_actor", "shared")
    return {"fit": fit, **{k: collapse(str(data.get(k) or "")) for k in keys}}


def plan_passes(v: dict[str, Any], block_a: str, block_b: str, aliases: list[str]) -> dict:
    """계획 이행 확인 응답의 근거를 모델이 본 이슈 요약문과 대조하고 {"pass", "why"} 를 돌려준다."""

    if v["fit"] != "THE_ITEM":
        return {"pass": False, "why": v["fit"]}
    if not grounded(v["a_quote"], block_a):
        return {"pass": False, "why": "a_quote not in A"}
    if not grounded(v["b_quote"], block_b):
        return {"pass": False, "why": "b_quote not in B"}
    b_side = v["b_quote"] + "\n" + headlines(block_b)
    if not shared_object(v["shared"], v["a_quote"], b_side, aliases):
        return {"pass": False, "why": "no shared object"}
    if not same_actor(v["a_actor"], v["b_actor"]):
        return {"pass": False, "why": "other actor"}
    return {"pass": True, "why": "THE_ITEM grounded"}


# ── 투표 ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Confirmation:
    """judge(scope 판정, 통과하지 못하면 계획 이행 확인)와 check 의 결과다."""

    judge_pass: bool
    judge_label: str
    judge_by: str
    a_scope: str
    b_scope: str
    plan_why: str | None
    check_pass: bool
    check_role: str


def confirm(a: Issue, b: Issue, client: LlmClient, names: dict[int, CompanyName]) -> Confirmation:
    """제안된 쌍 (A, B) 하나에 대해 확인자 표를 받는다."""

    judge_reply = client.call(
        "judge",
        system=SYSTEM,
        user=judge_prompt(a, b, names),
        max_tokens=JUDGE_MAX_TOKENS,
        cluster_id=b.id,
        candidate_id=a.id,
    )
    judged = parse_judge(judge_reply.parser_text())
    judge_pass = judge_passes(judged)
    label = judged["label"]
    judge_by = "scope_judge"
    plan_why = None
    if not judge_pass:
        block_a, block_b = blocks(a, b, names)
        plan_reply = client.call(
            "plan_reader",
            system=SYSTEM,
            user=plan_prompt(a, b, names),
            max_tokens=PLAN_MAX_TOKENS,
            cluster_id=b.id,
            candidate_id=a.id,
        )
        aliases = company_aliases(issue_company_ids(a) | issue_company_ids(b), names)
        verdict = plan_passes(parse_plan(plan_reply.parser_text()), block_a, block_b, aliases)
        judge_by = "plan_reader"
        plan_why = verdict["why"]
        if verdict["pass"]:
            judge_pass = True
            label = "SAME_STORY"

    check_reply = client.call(
        "check",
        system=SYSTEM,
        user=check_prompt(a, b, names),
        max_tokens=CHECK_MAX_TOKENS,
        cluster_id=b.id,
        candidate_id=a.id,
    )
    checked = parse_check(check_reply.parser_text())
    return Confirmation(
        judge_pass=judge_pass,
        judge_label=label,
        judge_by=judge_by,
        a_scope=judged["a_scope"],
        b_scope=judged["b_scope"],
        plan_why=plan_why,
        check_pass=check_passes(checked),
        check_role=checked["role"],
    )
