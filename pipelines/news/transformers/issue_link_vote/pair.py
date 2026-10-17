"""pair 투표자(제안자)다. 대상 이슈 B 하나에 대해 후보 A 를 한 쌍씩 판정한다.

후보는 후보 범위 규칙(universe.py)을 통과한 앞선 이슈 중 코사인 상위 12개다. 코사인이 같으면 늦게
시작한 쪽, 그다음 큰 id 를 앞에 둔다.

모든 쌍은 먼저 screen(1차 거르기)을 거친다(SCREEN_PROMPT, 이슈 요약문, 온도 0).

route 1(두 관점 프롬프트를 차례로 거친다):
  verify  evidence 관점 프롬프트(EVIDENCE_TOOL_PROMPT, 도구 강제 호출)로 3회 호출한다. 0번은 온도
          0, 1·2번은 온도 1 이다. 응답 2개 이상이 SAME_EVENT/SAME_STORY 면 통과한다.
  matter  matter 관점 프롬프트(MATTER_PROMPT)로 3회 호출한다. 응답 2개 이상이 긍정 라벨이고
          근거(basis)가 same_occurrence·specific_step·broad_step 이면 통과한다.

route 2(계획 경로)는 route 1 이 부모를 하나도 주지 못한 대상에만 돈다. 연결을 더할 뿐 route 1
결과를 바꾸지 않는다:
  plans      A 하나만 읽고 A 가 밝힌 구체적 계획을 뽑는다. 읽는 필드(제목, 한 줄 요약, 요약 300자)는
             B 의 시각과 무관하므로 A 마다 한 번만 부르고, 캐시된 결과를 다른 B 에도 그대로 쓴다.
  planstep   A 의 계획 중 하나의 "who" 가 B 의 주요 기업을 가리킬 때만 묻는다. 3회 호출 응답이 모두
             통과해야 한다. 코드는 relation=carries_out 이고 라벨이 SAME_STORY 인지, 계획 번호가
             맞는지, 그 계획의 who 가 B 의 주요 기업인지, 인용한 b_words 가 B 의 제목·한 줄 요약·
             기사 제목에 실제로 있는지 확인한다.
  evidence3  evidence 관점 프롬프트 v3(EVIDENCE3_TOOL_PROMPT)의 3회 호출 응답이 모두 긍정 라벨이어야
             한다.

라벨: route 1 로 통과하면 verify 응답 2개 이상이 SAME_EVENT 일 때 SAME_EVENT, 아니면 SAME_STORY
다. route 2 로 통과하면 SAME_STORY 다.

시점: A 의 이슈 요약문에는 B 의 첫 기사 시각까지 나온 멤버 기사만 보인다(as_of). B 보다 나중에 나온
기사가 판정에 섞이지 않게 하기 위해서다.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pipelines.news.transformers.issue_link_vote.issue import (
    CompanyName,
    Issue,
    collapse,
    fmt_minutes,
    gap_hours,
    kst,
)
from pipelines.news.transformers.issue_link_vote.llm import LlmClient, parallel_map
from pipelines.news.transformers.issue_link_vote.universe import Candidate
from pipelines.news.transformers.prompts.issue_link_pair import (
    EVIDENCE3_TOOL,
    EVIDENCE3_TOOL_PROMPT,
    EVIDENCE_TOOL,
    EVIDENCE_TOOL_PROMPT,
    MATTER_ACCEPTED,
    MATTER_PROMPT,
    MATTER_TOOL,
    PLAN_EXTRACT_PROMPT,
    PLAN_EXTRACT_TOOL,
    PLANSTEP_PROMPT,
    PLANSTEP_TOOL,
    SCREEN_PROMPT,
)

TOP_K = 12
SCREEN_MAX_TOKENS = 400
TOOL_MAX_TOKENS = 800
EVIDENCE3_MAX_TOKENS = 1200
# 호출 번호 0번은 온도 0, 1번부터는 온도 1 로 부른다.
SAMPLES = (0, 1, 2)
MIN_AGREE = 2
# 계획 경로는 route 1 이 모두 거절한 대상에 연결을 더하므로 두 점검 모두 만장일치여야 한다.
PLAN_MIN_AGREE = 3
POSITIVE = ("SAME_EVENT", "SAME_STORY")
LABELS = ("SAME_EVENT", "SAME_STORY", "DIFFERENT")

# 이슈 요약문에는 제목, 기간, 기사 수, 기업, 한 줄 요약, 요약 300자, 기사 제목 6개를 적는다.
NODE_MAX_TITLES = 6
NODE_SUMMARY_CHARS = 300
MAX_OTHER_COMPANIES = 8


def select_candidates(candidates: list[Candidate], top_k: int = TOP_K) -> list[Candidate]:
    """코사인 내림차순(같으면 늦게 시작한 쪽, 그다음 큰 id)으로 정렬해 앞 top_k 개를 고른다."""

    ranked = sorted(
        candidates,
        key=lambda c: (-c.cosine, -c.issue.first_published_at.timestamp(), -c.id),
    )
    return ranked[:top_k]


# ── 이슈 요약문 ─────────────────────────────────────────────────────────────


def _excerpt(text: str, limit: int | None) -> str:
    if limit is None or len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _fmt_short(value: datetime) -> str:
    return kst(value).strftime("%m-%d %H:%M")


def format_gap(hours: float) -> str:
    if hours < 48:
        return f"{hours:.0f} hours"
    return f"{hours / 24:.0f} days"


def as_of(issue: Issue, cutoff: datetime | None) -> Issue:
    """cutoff 시각에 독자가 볼 수 있던 이슈를 만든다. 그때까지 나온 멤버 기사만 남기고, 하나도
    없으면 가장 이른 기사 하나를 남긴다.

    제목·요약 같은 텍스트 필드는 그 시각의 값으로 되돌릴 수 없으므로 그대로 둔다.
    """

    if cutoff is None:
        return issue
    members = tuple(m for m in issue.members if m.published_at <= cutoff)
    if not members:
        members = tuple(sorted(issue.members, key=lambda m: m.published_at)[:1])
    if len(members) == len(issue.members):
        return issue
    last = max(members, key=lambda m: m.published_at).published_at
    return issue.with_members(members, last)


def issue_card(issue: Issue) -> str:
    """이슈 요약문을 만든다. 요약 문단은 300자까지만 발췌하므로 그 뒤에만 있는 정보는 보이지
    않는다."""

    primary = [issue.companies.get(c, str(c)) for c in issue.primary_company_ids]
    others = [name for cid, name in issue.companies.items() if cid not in issue.primary]
    lines = [
        f"Title: {collapse(issue.title)}",
        f"Articles: {issue.member_count}, published "
        f"{fmt_minutes(issue.first_published_at)} to {fmt_minutes(issue.last_published_at)} (KST)",
        f"Main companies: {', '.join(primary) if primary else '(none)'}",
    ]
    if others:
        lines.append(f"Other tagged companies: {', '.join(others[:MAX_OTHER_COMPANIES])}")
    node = collapse(issue.node_summary)
    if node:
        lines.append(f"Headline point: {node}")
    summary = collapse(issue.summary)
    if summary:
        lines.append(f"Summary: {_excerpt(summary, NODE_SUMMARY_CHARS)}")
    seen: set[str] = set()
    titles = []
    for member in issue.members:
        title = collapse(member.title)
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append(f"- [{_fmt_short(member.published_at)}] {title}")
    lines.append("Article titles:")
    lines.extend(titles[:NODE_MAX_TITLES])
    if len(titles) > NODE_MAX_TITLES:
        lines.append(f"- ... and {len(titles) - NODE_MAX_TITLES} more")
    return "\n".join(lines)


def pair_message(parent: Issue, child: Issue, gap: float) -> str:
    """screen 단계의 사용자 메시지다. A 는 B 의 첫 기사 시각까지 나온 기사로만 보인다."""

    shown_parent = as_of(parent, child.first_published_at)
    return (
        "Issue A (earlier):\n"
        f"{issue_card(shown_parent)}\n\n"
        f"Issue B (later; its first article came {format_gap(gap)} after A's first article):\n"
        f"{issue_card(child)}\n\n"
        "Classify B's relation to A."
    )


def pair_message_r2(parent: Issue, child: Issue, gap: float) -> str:
    """관점 판정 단계(verify, matter, evidence3)의 사용자 메시지다."""

    shown_parent = as_of(parent, child.first_published_at)
    return (
        "Issue A (earlier):\n"
        f"{issue_card(shown_parent)}\n\n"
        f"Issue B (later; its first article came {format_gap(gap)} after A's first article):\n"
        f"{issue_card(child)}\n\n"
        "Record your verdict on whether B continues A's story."
    )


def plan_source_text(issue: Issue) -> str:
    """계획 추출이 읽는 필드다. B 의 시각에 따라 달라지는 필드가 없으므로 A 마다 한 번만
    추출한다."""

    lines = [f"Title: {collapse(issue.title)}"]
    node = collapse(issue.node_summary)
    if node:
        lines.append(f"Headline point: {node}")
    summary = collapse(issue.summary)
    if summary:
        lines.append(f"Summary: {_excerpt(summary, 300)}")
    return "\n".join(lines)


def plan_extract_message(issue: Issue) -> str:
    return f"Issue:\n{plan_source_text(issue)}\n\nRecord the concrete plans that this issue states."


def format_plans(plans: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{k}. who: {collapse(p.get('who'))} | plan: {collapse(p.get('plan'))} | object: "
        f"{collapse(p.get('named_object'))}"
        for k, p in enumerate(plans, 1)
    )


def planstep_message(parent: Issue, child: Issue, gap: float, plans: list[dict[str, Any]]) -> str:
    shown_parent = as_of(parent, child.first_published_at)
    return (
        "Issue A (earlier):\n"
        f"{issue_card(shown_parent)}\n\n"
        f"Concrete plans that A states (listed from A alone):\n{format_plans(plans)}\n\n"
        f"Issue B (later; its first article came {format_gap(gap)} after A's first article):\n"
        f"{issue_card(child)}\n\n"
        "Record whether B's own news carries out one of A's plans."
    )


# ── 코드 점검 ───────────────────────────────────────────────────────────────


def _squash(text: str | None) -> str:
    return "".join(ch for ch in (text or "") if ch.isalnum())


def b_own_text(child: Issue) -> str:
    """B 자신의 소식으로 제목, 한 줄 요약, 기사 제목을 이어 붙인다. 요약 문단은 앞선 사안을 배경으로
    되풀이하는 일이 많아 뺀다."""

    parts = [child.title, child.node_summary] + [m.title for m in child.members]
    return " ".join(collapse(p) for p in parts if p)


def planstep_accepts(verdict: dict[str, Any], plans: list[dict[str, Any]], child: Issue) -> bool:
    """planstep 판정을 코드로 점검한다. relation 이 carries_out, 라벨이 SAME_STORY 이고, 계획 번호가
    범위 안이며, b_words 가 B 자신의 소식에 있어야 한다."""

    idx = verdict.get("plan_index")
    words = _squash(verdict.get("b_words"))
    return (
        verdict.get("relation") == "carries_out"
        and verdict.get("label") == "SAME_STORY"
        and isinstance(idx, int)
        and 1 <= idx <= len(plans)
        and len(words) >= 2
        and words in _squash(b_own_text(child))
    )


def company_names_of(issue: Issue, names: dict[int, CompanyName]) -> list[str]:
    """이슈 주요 기업의 이름과 별칭을 모은다. 주요 기업이 없으면 연결 기업 전부를 쓴다."""

    ids = list(issue.primary_company_ids) or list(issue.companies)
    out: list[str] = []
    for cid in ids:
        entry = names.get(cid)
        name = (entry.name if entry else "") or issue.companies.get(cid, "")
        out.extend([name, *(entry.aliases if entry else ())])
    return [n for n in out if n]


def plan_who_matches(plan: dict[str, Any], child: Issue, names: dict[int, CompanyName]) -> bool:
    """계획의 "who" 가 B 의 주요 기업을 가리키는지 본다. 계획 경로는 같은 회사가 자기 계획을
    실행한 경우만 잇는다."""

    who = _squash(plan.get("who"))
    return any(len(_squash(n)) >= 2 and _squash(n) in who for n in company_names_of(child, names))


# ── 응답 해석 ───────────────────────────────────────────────────────────────


def parse_verdict(text: str) -> dict[str, Any]:
    """screen 의 자유 텍스트 JSON 응답을 해석한다. JSON 을 읽지 못하면 본문에 마지막으로 나온 라벨을
    쓰고, 라벨도 없으면 DIFFERENT 로 본다."""

    match = re.search(r"\{.*\}", text, flags=re.S)
    if match:
        try:
            obj = json.loads(match.group(0))
            label = str(obj.get("label", "")).strip().upper()
            if label in LABELS:
                obj["label"] = label
                return obj
        except json.JSONDecodeError:
            pass
    found = re.findall(r"SAME_EVENT|SAME_STORY|DIFFERENT", text)
    return {"label": found[-1] if found else "DIFFERENT", "parse_fallback": True}


def parse_tool_verdict(
    row: dict[str, Any], accepted_bases: frozenset[str] | None = None
) -> dict[str, Any]:
    """도구 호출 판정을 해석한다. 응답이 없거나 잘렸거나 형식이 틀리면 DIFFERENT 로 본다.

    accepted 는 라벨이 긍정일 때 True 다. accepted_bases 를 주면(근거 필드가 닫힌 목록인 도구)
    근거도 그 안에 있어야 한다. evidence 도구의 same_specific_matter 는 참고용이라 보지 않는다.
    실적 시리즈는 이 값이 false 로 오지만 프롬프트는 연결로 판정하기 때문이다.
    """

    obj = row.get("tool_input")
    if not isinstance(obj, dict) or row.get("stop_reason") == "max_tokens":
        return {"label": "DIFFERENT", "invalid": True, "accepted": False}
    out = dict(obj)
    label = str(out.get("label", "")).strip().upper()
    out["label"] = label if label in LABELS else "DIFFERENT"
    accepted = out["label"] in POSITIVE
    if accepted_bases is not None:
        accepted = accepted and out.get("basis") in accepted_bases
    out["accepted"] = accepted
    return out


def agreed(samples: list[dict[str, Any]], need: int = MIN_AGREE) -> bool:
    return sum(1 for s in samples if s["accepted"]) >= need


# ── 단계 ────────────────────────────────────────────────────────────────────


@dataclass
class PairStages:
    """후보(부모 id)별 단계 판정을 모은다. plans 는 route 2 에서 screen 을 통과한 후보마다 추출한
    계획이다."""

    candidates: list[Candidate]
    screen: dict[int, dict[str, Any]] = field(default_factory=dict)
    verify: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    matter: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    plans: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    planstep: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    evidence3: dict[int, list[dict[str, Any]]] = field(default_factory=dict)

    def route1(self, a: int) -> bool:
        screen = self.screen.get(a)
        if screen is None or screen["label"] not in POSITIVE:
            return False
        verify = self.verify.get(a)
        if verify is None or not agreed(verify):
            return False
        matter = self.matter.get(a)
        return matter is not None and agreed(matter)

    def route2(self, a: int) -> bool:
        screen = self.screen.get(a)
        if screen is None or screen["label"] not in POSITIVE:
            return False
        step = self.planstep.get(a)
        if step is None or not agreed(step, PLAN_MIN_AGREE):
            return False
        ev = self.evidence3.get(a)
        return ev is not None and agreed(ev, PLAN_MIN_AGREE)

    def verdicts(self) -> dict[int, dict[str, Any]]:
        """부모 id 마다 {"pass", "label", "route"} 를 돌려준다. 통과하지 못하면 route 는 0 이다."""

        out: dict[int, dict[str, Any]] = {}
        for cand in self.candidates:
            a = cand.id
            r1, r2 = self.route1(a), self.route2(a)
            label = "DIFFERENT"
            if r1:
                events = sum(1 for s in self.verify[a] if s["label"] == "SAME_EVENT")
                label = "SAME_EVENT" if events >= MIN_AGREE else "SAME_STORY"
            elif r2:
                label = "SAME_STORY"
            out[a] = {"pass": r1 or r2, "label": label, "route": 1 if r1 else (2 if r2 else 0)}
        return out

    def evidence(self, a: int) -> dict[str, Any]:
        """투표 기록에 남길 단계별 라벨과 통과 여부를 모은다."""

        out: dict[str, Any] = {}
        if a in self.screen:
            out["screen"] = self.screen[a]["label"]
        for name in ("verify", "matter", "planstep", "evidence3"):
            samples = getattr(self, name).get(a)
            if samples is not None:
                out[name] = [
                    {"label": s.get("label"), "accepted": bool(s.get("accepted"))} for s in samples
                ]
        if a in self.plans:
            out["plans"] = len(self.plans[a])
        return out


def _temperature(sample: int) -> float:
    return 0.0 if sample == 0 else 1.0


def classify(
    child: Issue,
    candidates: list[Candidate],
    client: LlmClient,
    names: dict[int, CompanyName],
    workers: int,
) -> PairStages:
    """B 하나에 대해 pair 단계를 모두 돌린다. candidates 는 select_candidates 결과다."""

    stages = PairStages(candidates=candidates)
    by_id = {c.id: c.issue for c in candidates}
    gaps = {c.id: gap_hours(c.issue, child) for c in candidates}

    def screen(a: int) -> dict[str, Any]:
        reply = client.call(
            "screen",
            system=SCREEN_PROMPT,
            user=pair_message(by_id[a], child, gaps[a]),
            max_tokens=SCREEN_MAX_TOKENS,
            cluster_id=child.id,
            candidate_id=a,
        )
        return parse_verdict(reply.parser_text())

    def lens(stage: str, system: str, tool: dict, bases, max_tokens: int):
        def run(a: int) -> list[dict[str, Any]]:
            user = pair_message_r2(by_id[a], child, gaps[a])
            out = []
            for s in SAMPLES:
                reply = client.call(
                    stage,
                    system=system,
                    user=user,
                    max_tokens=max_tokens,
                    temperature=_temperature(s),
                    sample=s,
                    tool=tool,
                    cluster_id=child.id,
                    candidate_id=a,
                )
                verdict = parse_tool_verdict(reply.tool_row(), bases)
                verdict["sample"] = s
                out.append(verdict)
            return out

        return run

    ids = [c.id for c in candidates]
    stages.screen = dict(zip(ids, parallel_map(screen, ids, workers), strict=True))
    screened = [a for a in ids if stages.screen[a]["label"] in POSITIVE]

    verify = lens("verify.evidence", EVIDENCE_TOOL_PROMPT, EVIDENCE_TOOL, None, TOOL_MAX_TOKENS)
    stages.verify = dict(zip(screened, parallel_map(verify, screened, workers), strict=True))
    agreed_ids = [a for a in screened if agreed(stages.verify[a])]
    matter = lens("verify.matter", MATTER_PROMPT, MATTER_TOOL, MATTER_ACCEPTED, TOOL_MAX_TOKENS)
    stages.matter = dict(zip(agreed_ids, parallel_map(matter, agreed_ids, workers), strict=True))

    # route 2 는 route 1 이 부모를 주지 못한 대상에만 돈다.
    if any(stages.route1(a) for a in screened):
        return stages

    def extract(a: int) -> list[dict[str, Any]]:
        reply = client.call(
            "plan_extract",
            system=PLAN_EXTRACT_PROMPT,
            user=plan_extract_message(by_id[a]),
            max_tokens=TOOL_MAX_TOKENS,
            tool=PLAN_EXTRACT_TOOL,
            cluster_id=a,
        )
        row = reply.tool_row()
        found = (
            (row.get("tool_input") or {}).get("plans")
            if row.get("stop_reason") != "max_tokens"
            else None
        )
        return [p for p in (found or []) if isinstance(p, dict)]

    stages.plans = dict(zip(screened, parallel_map(extract, screened, workers), strict=True))

    def planstep(a: int) -> list[dict[str, Any]]:
        plans = stages.plans[a]
        user = planstep_message(by_id[a], child, gaps[a], plans)
        out = []
        for s in SAMPLES:
            reply = client.call(
                "plan_step.planstep",
                system=PLANSTEP_PROMPT,
                user=user,
                max_tokens=TOOL_MAX_TOKENS,
                temperature=_temperature(s),
                sample=s,
                tool=PLANSTEP_TOOL,
                cluster_id=child.id,
                candidate_id=a,
            )
            row = reply.tool_row()
            v = dict(row.get("tool_input") or {}) if row.get("stop_reason") != "max_tokens" else {}
            v["accepted"] = (
                bool(v)
                and planstep_accepts(v, plans, child)
                and plan_who_matches(plans[v["plan_index"] - 1], child, names)
            )
            v["sample"] = s
            out.append(v)
        return out

    evidence3 = lens(
        "plan_step.evidence3", EVIDENCE3_TOOL_PROMPT, EVIDENCE3_TOOL, None, EVIDENCE3_MAX_TOKENS
    )
    jobs = [a for a in screened if any(plan_who_matches(p, child, names) for p in stages.plans[a])]
    stages.planstep = dict(zip(jobs, parallel_map(planstep, jobs, workers), strict=True))
    jobs = [a for a in jobs if agreed(stages.planstep[a], PLAN_MIN_AGREE)]
    stages.evidence3 = dict(zip(jobs, parallel_map(evidence3, jobs, workers), strict=True))
    return stages
