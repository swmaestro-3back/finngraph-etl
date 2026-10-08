"""rank 투표자(제안자)다. 대상 이슈 B 에 후보 여러 개를 한 번에 보여 주고, 행마다 받은 라벨을
check_affirm 으로 점검한다.

후보: 후보 범위 규칙(universe.py)을 통과한 앞선 이슈를 주요 기업이 겹치는 것, 연결 기업이 겹치는
것, 나머지 순으로 묶고, 묶음 안에서 코사인 내림차순(같으면 늦게 시작한 쪽)으로 세워 앞 10개를
고른다.

대상 이슈 하나에 후보 목록 전체를 보는 호출을 한 번 한다(SYSTEM_V8, 온도 0). 이슈 요약문에는 제목,
첫 기사 시각, 기업, 한 줄 요약, 요약 400자, 기사 제목 6개를 적는다. 후보의 이슈 요약문에는 B 의
마지막 기사 시각까지 나온 기사만 보인다. 그 뒤에도 기사가 늘어난 후보는 한 줄 요약과 요약도
가린다(causal="strict").

후보의 통과 조건: 모델이 그 행에 SAME_EVENT 나 SAME_STORY 를 붙였고 check_affirm 이 받아들여야
한다.
  - SAME_STORY: new_matter 와 old_matter 가 두 이슈의 공통 주요 기업 이름이 아닌 단어를 공유하고,
    step_by 가 party·authority·schedule 이며, key 의 모든 단어가 두 이슈의 제목 텍스트(이슈
    제목·한 줄 요약·기사 제목)에 있다.
  - SAME_EVENT: 첫 기사 간격이 168시간 이하이고, matter 단어(공통 주요 기업 제외)를 두 matter 가
    공유하거나 그 단어가 두 이슈의 제목 텍스트에 있다.
  - 구조 규칙(주요 기업을 공유하고 코사인 0.40 이상이거나, 둘 다 기업이 없고 코사인 0.75 이상)을
    만족하지 않는 후보는 더 엄격한 점검(check_candidate)도 통과해야 한다.
형식 재요청 뒤에도 검증에 실패한 응답은 아무 후보도 제안하지 않는다.
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
    fmt_minutes,
    gap_hours,
)
from pipelines.news.transformers.issue_link_vote.llm import LlmClient
from pipelines.news.transformers.issue_link_vote.universe import Candidate
from pipelines.news.transformers.prompts.issue_link_rank import SYSTEM_V8

TOP_N = 10
MAX_MEMBER_TITLES = 6
SUMMARY_CHARS = 400
# 출력 토큰 상한이다. 실제로 생성한 토큰만 과금되므로 넉넉하게 둔다.
MAX_TOKENS = 2048
RELATION = {"SAME_EVENT": "same_event", "SAME_STORY": "follow_up"}

STRUCT_MIN_COSINE = 0.40
STRUCT_NO_COMPANY_MIN_COSINE = 0.75
EVENT_MAX_GAP_HOURS = 168.0
EVENT_TITLE_SHARE = 0.5
STEP_BY_OK = ("party", "authority")
STEP_BY_OK_V8 = ("party", "authority", "schedule")
_SPLIT = re.compile(r"[\s·,/|]+")


@dataclass(frozen=True)
class RankCandidate:
    issue: Issue
    cosine: float
    group: int
    gap_hours: float

    @property
    def id(self) -> int:
        return self.issue.id


def select_candidates(
    child: Issue, candidates: list[Candidate], top_n: int = TOP_N
) -> list[RankCandidate]:
    """주요 기업 공유(0), 연결 기업 공유(1), 나머지(2) 순으로 묶고, 묶음 안에서는 코사인
    내림차순으로 세워 앞 top_n 개를 고른다."""

    out = []
    for cand in candidates:
        parent = cand.issue
        shares_any = bool(parent.linked & child.linked)
        shares_primary = bool(parent.primary & child.primary)
        group = 0 if shares_primary else (1 if shares_any else 2)
        out.append(RankCandidate(parent, cand.cosine, group, gap_hours(parent, child)))
    out.sort(key=lambda c: (c.group, -c.cosine, -c.issue.first_published_at.timestamp(), -c.id))
    return out[:top_n]


# ── 이슈 요약문 ─────────────────────────────────────────────────────────────


def _company_names(issue: Issue) -> list[str]:
    keys = sorted(issue.companies, key=lambda c: (c not in issue.primary, issue.companies[c]))
    return [issue.companies[c] for c in keys]


def card_parts(issue: Issue, cutoff: datetime | None = None, causal: str = "snapshot") -> dict:
    """이슈 요약문에 보일 필드를 고른다. cutoff 뒤에 나온 멤버 기사는 가린다.

    causal 이 "strict" 이면 cutoff 뒤에도 기사가 늘어난 이슈의 한 줄 요약과 요약도 가린다. 그 늦은
    기사까지 보고 만든 값이기 때문이다. 제목은 이전 값을 남겨 두지 않으므로 그대로 보인다.
    """

    seen: set[str] = set()
    titles: list[tuple[str, str]] = []
    for member in sorted(issue.members, key=lambda m: m.published_at):
        if cutoff is not None and member.published_at > cutoff:
            continue
        title = collapse(member.title)
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append((fmt_minutes(member.published_at)[5:], title))
        if len(titles) == MAX_MEMBER_TITLES:
            break
    grew_later = cutoff is not None and issue.last_published_at > cutoff
    hide_generated = causal == "strict" and grew_later
    summary = collapse(issue.summary)
    if len(summary) > SUMMARY_CHARS:
        summary = summary[: SUMMARY_CHARS - 1] + "…"
    return {
        "title": collapse(issue.title),
        "first": fmt_minutes(issue.first_published_at),
        "companies": _company_names(issue),
        "key_point": None if hide_generated else (collapse(issue.node_summary) or None),
        "summary": None if hide_generated else (summary or None),
        "titles": titles,
    }


def render(parts: dict) -> str:
    lines = [
        f"Title: {parts['title']}",
        f"First article: {parts['first']} KST",
        f"Companies: {', '.join(parts['companies']) or '(none)'}",
    ]
    if parts["key_point"]:
        lines.append(f"Key point: {parts['key_point']}")
    if parts["summary"]:
        lines.append(f"Summary: {parts['summary']}")
    if parts["titles"]:
        lines.append("Article titles:")
        lines.extend(f"  - {ts} {title}" for ts, title in parts["titles"])
    return "\n".join(lines)


def _gap_text(hours: float) -> str:
    if hours < 48:
        return f"{hours:.0f} hours"
    return f"{hours / 24:.0f} days"


def listwise_user(child: Issue, cands: list[RankCandidate]) -> str:
    cutoff = child.last_published_at
    parts = ["NEW issue", render(card_parts(child)), "", f"EARLIER candidates ({len(cands)}):"]
    for k, cand in enumerate(cands, start=1):
        parts.append("")
        parts.append(f"C{k} (first article {_gap_text(cand.gap_hours)} before the NEW issue)")
        parts.append(render(card_parts(cand.issue, cutoff, "strict")))
    return "\n".join(parts)


def parse_json(text: str) -> dict[str, Any] | None:
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def rows_by_number(parsed: dict[str, Any]) -> dict[int, dict[str, Any]]:
    rows = {}
    for row in parsed.get("candidates") or []:
        try:
            rows[int(str(row.get("c")).lstrip("Cc"))] = row
        except ValueError:
            continue
    return rows


# ── 점검 ────────────────────────────────────────────────────────────────────


def _norm(text: str | None) -> str:
    return "".join(ch for ch in (text or "").lower() if ch.isalnum())


def _words(text: Any) -> list[str]:
    if not isinstance(text, str):
        return []
    return [w for w in (_norm(part) for part in _SPLIT.split(text)) if w]


def _terms(companies: dict[int, str], names: dict[int, CompanyName]) -> set[str]:
    terms: set[str] = set()
    for cid, name in companies.items():
        terms.add(_norm(name))
        info = names.get(cid)
        if info:
            for value in [info.name, info.ticker, *info.aliases]:
                if value:
                    terms.add(_norm(value))
    terms.discard("")
    return terms


def company_terms(names: dict[int, CompanyName], *issues: Issue) -> set[str]:
    """이슈들의 연결 기업 이름, 종목코드, 별칭을 정규화해 모은다."""

    terms: set[str] = set()
    for issue in issues:
        terms |= _terms(issue.companies, names)
    return terms


def _is_company_word(word: str, terms: set[str]) -> bool:
    return any(word == t or word in t or (len(t) >= 2 and t in word) for t in terms)


def headline_text(parts: dict) -> str:
    return _norm(
        " ".join([parts["title"], parts["key_point"] or "", *(t for _, t in parts["titles"])])
    )


def main_text(parts: dict) -> str:
    return _norm(" ".join([parts["title"], parts["key_point"] or ""]))


def verify_key(
    key: Any, new_head: str, old_head: str, terms: set[str], strict: bool = True
) -> tuple[bool, str]:
    """key 가 두 이슈의 제목 텍스트에 근거를 두는지 본다. strict 이면 key 의 모든 단어가 두 제목
    텍스트에 있고, 그중 하나 이상은 기업 이름이 아닌 2글자 이상 단어여야 한다. strict 가
    아니면(구조 규칙을 만족하는 후보) 2글자 이상 key 단어 하나만 두 제목 텍스트에 있으면 된다."""

    words = _words(key)
    if not words:
        return False, "no key"
    shared = [w for w in words if w in new_head and w in old_head]
    if not strict:
        if any(len(w) >= 2 for w in shared):
            return True, "key ok (shared word)"
        return False, "no key word in both headlines"
    missing = [w for w in words if w not in shared]
    if missing:
        return False, f"key words not in both headlines: {missing}"
    if not any(len(w) >= 2 and not _is_company_word(w, terms) for w in words):
        return False, "key is only company names or 1-character words"
    return True, "key ok"


def verify_stage(
    stage: Any, key: Any, new_head: str, old_main: str, terms: set[str]
) -> tuple[bool, str]:
    words = _words(stage)
    if not words:
        return False, "no stage"
    missing = [w for w in words if w not in new_head]
    if missing:
        return False, f"stage words not in NEW headlines: {missing}"
    key_words = set(_words(key))
    novel = [
        w
        for w in words
        if len(w) >= 2
        and not _is_company_word(w, terms)
        and w not in key_words
        and w not in old_main
    ]
    if not novel:
        return False, "stage adds nothing beyond the earlier title and key point"
    return True, "stage ok"


def verify_event_titles(new_title: str, old_head: str) -> tuple[bool, str]:
    """같은 사건인지 텍스트로 확인한다. NEW 제목 단어의 절반 이상이 앞선 이슈의 제목 텍스트에
    있어야 한다."""

    words = [w for w in _words(new_title) if len(w) >= 2]
    found = sum(w in old_head for w in words)
    ok = bool(words) and found / len(words) >= EVENT_TITLE_SHARE
    return ok, f"title words shared {found}/{len(words)}"


def is_structural(child: Issue, cand: RankCandidate) -> bool:
    """구조 규칙(모듈 설명 참고)을 만족하는 후보인지 본다."""

    if cand.group == 0 and cand.cosine >= STRUCT_MIN_COSINE:
        return True
    return (
        not child.companies
        and not cand.issue.companies
        and cand.cosine >= STRUCT_NO_COMPANY_MIN_COSINE
    )


def check_candidate(
    child: Issue, cand: RankCandidate, row: dict[str, Any], names: dict[int, CompanyName]
) -> dict[str, Any]:
    """구조 규칙을 만족하지 않는 후보에 적용하는 엄격한 점검이다."""

    label = str(row.get("label", "")).upper()
    if label not in RELATION:
        return {"ok": False, "label": label, "why": f"labelled {label}"}
    cutoff = child.last_published_at
    parent = cand.issue
    new_parts = card_parts(child)
    old_parts = card_parts(parent, cutoff, "strict")
    terms = company_terms(names, child, parent)
    new_head, old_head = headline_text(new_parts), headline_text(old_parts)
    structural = is_structural(child, cand)
    path = "struct" if structural else ("event" if label == "SAME_EVENT" else "stage")
    ok, why = verify_key(row.get("key"), new_head, old_head, terms, strict=not structural)
    if not ok:
        return {"ok": False, "label": label, "why": why}
    if label == "SAME_EVENT":
        if structural:
            return {"ok": True, "label": label, "path": path, "why": why}
        if cand.gap_hours > EVENT_MAX_GAP_HOURS:
            return {
                "ok": False,
                "label": label,
                "why": f"SAME_EVENT without structure, gap {cand.gap_hours:.0f}h",
            }
        ok, why2 = verify_event_titles(new_parts["title"], old_head)
        return {"ok": ok, "label": label, "path": path if ok else None, "why": f"{why}; {why2}"}
    if row.get("step_by") not in STEP_BY_OK:
        return {"ok": False, "label": label, "why": f"{why}; step_by {row.get('step_by')!r}"}
    ok, why2 = verify_stage(row.get("stage"), row.get("key"), new_head, main_text(old_parts), terms)
    return {"ok": ok, "label": label, "path": path if ok else None, "why": f"{why}; {why2}"}


def shared_primary_terms(child: Issue, parent: Issue, names: dict[int, CompanyName]) -> set[str]:
    """두 이슈 모두에서 주요 기업인 기업의 이름, 종목코드, 별칭을 모은다."""

    shared = child.primary & parent.primary
    companies = {c: child.companies.get(c) or parent.companies.get(c) or "" for c in shared}
    return _terms(companies, names)


def verify_matters(
    row: dict[str, Any],
    child: Issue,
    parent: Issue,
    names: dict[int, CompanyName],
    label: str = "SAME_STORY",
    new_head: str = "",
    old_head: str = "",
) -> tuple[bool, str]:
    """모델이 적은 두 matter 가 공통 주요 기업 이름이 아닌 내용 단어를 공유해야 한다. SAME_EVENT
    는 그런 matter 단어 하나가 두 이슈의 제목 텍스트에 모두 있어도 통과한다. 한 사건을 다룬 두
    보도가 서로 다른 측면을 matter 로 적을 수 있기 때문이다."""

    new_matter, old_matter = row.get("new_matter"), row.get("old_matter")
    terms = shared_primary_terms(child, parent, names)
    new_words = [w for w in _words(new_matter) if len(w) >= 2 and not _is_company_word(w, terms)]
    old_words = [w for w in _words(old_matter) if len(w) >= 2 and not _is_company_word(w, terms)]
    if not new_words or not old_words:
        return False, "no matter words beyond the shared company"
    new_norm, old_norm = _norm(new_matter), _norm(old_matter)
    shared = sorted(
        {w for w in new_words if w in old_norm} | {w for w in old_words if w in new_norm}
    )
    if shared:
        return True, f"matters share {shared}"
    if label == "SAME_EVENT":
        both = sorted({w for w in new_words + old_words if w in new_head and w in old_head})
        if both:
            return True, f"matter words in both headlines {both}"
    return False, "matters share no word"


def check_affirm(
    child: Issue, cand: RankCandidate, row: dict[str, Any], names: dict[int, CompanyName]
) -> dict[str, Any]:
    """후보 하나에 rank 통과 조건(모듈 설명 참고)을 적용한다."""

    label = str(row.get("label", "")).upper()
    if label not in RELATION:
        return {"ok": False, "label": label, "why": f"labelled {label or None}"}
    parent = cand.issue
    structural = is_structural(child, cand)
    cutoff = child.last_published_at
    new_head = headline_text(card_parts(child))
    old_head = headline_text(card_parts(parent, cutoff, "strict"))
    ok, why = verify_matters(row, child, parent, names, label, new_head, old_head)
    if not ok:
        return {"ok": False, "label": label, "why": why}
    if label == "SAME_EVENT" and cand.gap_hours > EVENT_MAX_GAP_HOURS:
        return {
            "ok": False,
            "label": label,
            "why": f"{why}; SAME_EVENT {cand.gap_hours:.0f}h apart",
        }
    if label == "SAME_STORY" and row.get("step_by") not in STEP_BY_OK_V8:
        return {"ok": False, "label": label, "why": f"{why}; step_by {row.get('step_by')!r}"}
    if label == "SAME_STORY":
        key_words = _words(row.get("key"))
        missing = [w for w in key_words if not (w in new_head and w in old_head)]
        if not key_words or missing:
            return {
                "ok": False,
                "label": label,
                "why": f"{why}; anchor not named in both headlines",
            }
    if structural:
        return {"ok": True, "label": label, "path": "struct", "why": why}
    rescue_row = {**row, "step_by": "party"} if label == "SAME_STORY" else row
    rescue = check_candidate(child, cand, rescue_row, names)
    return {**rescue, "why": f"{why}; {rescue['why']}"}


# ── 투표 ────────────────────────────────────────────────────────────────────


def verdicts(
    child: Issue,
    cands: list[RankCandidate],
    client: LlmClient,
    names: dict[int, CompanyName],
) -> dict[int, dict[str, Any]]:
    """부모 id 마다 {"pass", "label", "why"} 를 돌려준다. 후보가 없으면 LLM 을 부르지 않는다."""

    if not cands:
        return {}
    reply = client.call(
        "rank",
        system=SYSTEM_V8,
        user=listwise_user(child, cands),
        max_tokens=MAX_TOKENS,
        cluster_id=child.id,
    )
    parsed = parse_json(reply.parser_text())
    if not parsed:
        return {c.id: {"pass": False, "label": "UNPARSED", "why": reply.error} for c in cands}
    rows = rows_by_number(parsed)
    out: dict[int, dict[str, Any]] = {}
    for k, cand in enumerate(cands, start=1):
        row = rows.get(k, {})
        label = str(row.get("label", "")).upper()
        result = {"ok": False, "why": f"labelled {label or None}"}
        if label in RELATION:
            result = check_affirm(child, cand, row, names)
        out[cand.id] = {"pass": bool(result["ok"]), "label": label, "why": result.get("why")}
    return out
