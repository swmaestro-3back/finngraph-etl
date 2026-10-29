"""엔티티 필터 — 제목·본문의 후보 기업이 주된 뉴스의 당사자(party)인가 (jev).

후보(`_candidates`)는 company_matches.match_candidates 가 제목과 본문에서 찾은 상장사 중 검색
종목(관련성 필터가 판정했다)과 이미 연결된 기업을 뺀 것이다. 긴 단어의 일부('SK하이닉스' 의 SK)는
코드의 어절 규칙이 먼저 거르고, 남은 후보마다 is_company(Noul)·role(Choice) 두 질문을 한 요청에
펼쳐 묻는다. 통과한 기업을 `_linked_companies` 에 더한다. 결과는 기업 피드와 Event 당사자에 그대로
노출되므로 애매하면 버린다 — 임계값이 그 몫이다.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Literal

from pydantic import BaseModel, Field

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings

logger = get_logger(__name__)

# 후보 표기가 기사에서 맡은 역할. prompts.entity.ROLE_CRITERIA 의 네 옵션 + not_company
Role = Literal["party", "background", "listed", "source", "not_company"]

# 기업에 연결하는 역할. 나머지는 버린다
KEPT_ROLES = frozenset({"party"})

# 판정 임계값. 값은 NEWS_ENTITY_* 환경변수(기본값은 news.config.NewsSettings)로 정한다. 원시 확률은
# item 의 `_entity_judgements` 에 남으므로 골드로 재호출 없이 스윕할 수 있다
_settings = get_news_settings()
THRESHOLDS = {
    "company_min": _settings.entity_company_min,  # is_company 가 이 밑이면 not_company
    "party_min": _settings.entity_party_min,  # role 의 p(party) 가 이 이상이면 party
}

LEAD_PARAGRAPHS = 3
LEAD_CHARS = 800
SENTENCES_PER_CANDIDATE = 6


class EntityJudgement(BaseModel):
    """후보 표기 하나의 판정. `scores` 는 jev 원시 확률(평가·임계값 조정용). 어절 규칙으로
    버린 후보는 비어 있다."""

    entity: str
    role: Role
    scores: dict[str, float] = Field(default_factory=dict)


class EntityJudgementList(BaseModel):
    """후보 표기마다 판정 하나."""

    judgements: list[EntityJudgement]


# (제목, 검색 종목 이름(주인공), 본문, 후보 [{name: 사전 표준명, surface: 제목·본문 표기}]) → 판정.
# 판정의 entity 는 surface 다
Judge = Callable[[str, list[str], str, list[dict[str, str]]], Awaitable[EntityJudgementList]]


@dataclass
class EntityFilterResult:
    # jev 를 부른 기사 수 (후보가 전부 어절 규칙으로 빠진 기사도 센다)
    judged: int = 0
    # 그중 호출이 실패한 기사 수. 그 기사는 관련성을 통과한 검색 종목만 연결된다
    failed: int = 0
    # 통과한 후보 기업 수
    kept: int = 0
    # 판정받은 후보 표기의 역할별 수. 어떤 이유로 버려지는지 로그로 본다
    roles: Counter[str] = field(default_factory=Counter)
    # jev 호출마다(실패 포함) 걸린 시간. 런 요약 로그가 jev.latency_summary 로 묶는다
    latencies_ms: list[float] = field(default_factory=list)


def resolve_judgements(
    companies: list[dict[str, Any]], judgements: list[EntityJudgement]
) -> list[dict[str, Any]]:
    """판정을 기업으로 되돌린다. 통과한 기업을 후보 등장 순서로 돌려준다.

    역할이 KEPT_ROLES 인 표기가 하나라도 있는 기업만 통과다. 판정이 빠진 표기는 버린다. 입력에
    없는 이름은 무시한다. 같은 표기의 판정이 여러 번이면 첫 판정이 이긴다.
    """

    keep_by_surface: dict[str, bool] = {}
    for judgement in judgements:
        surface = judgement.entity.strip()
        if surface in keep_by_surface:
            continue
        keep_by_surface[surface] = judgement.role in KEPT_ROLES

    passed: dict[int, dict[str, Any]] = {}
    for company in companies:
        if keep_by_surface.get(company["surface"].strip(), False):
            passed.setdefault(
                company["company_id"],
                {"company_id": company["company_id"], "name": company["name"]},
            )

    return list(passed.values())


def merge_linked(
    query_linked: list[dict[str, Any]], candidates_passed: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """관련성을 통과한 검색 종목 뒤에 party 후보 기업을 잇는다. 같은 기업은 한 번만 든다."""

    merged: dict[int, dict[str, Any]] = {}
    for company in [*query_linked, *candidates_passed]:
        merged.setdefault(int(company["company_id"]), company)
    return list(merged.values())


# ── 문장·어절 (코드 단계) ───────────────────────────────────────────────────

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")

# 어절 경계: 공백, 가운뎃점·쉼표 나열, 괄호·꺾쇠·따옴표. 조사는 붙어 있어야 하므로 자르지 않는다
_TOKEN_BOUNDARY = set(" \t\n\r·,()[]<>'\"“”‘’/")
_TOKEN_STRIP = ".!?…:;"

# 회사명 뒤에 붙어도 회사명이 끝난 것으로 보는 조각. 긴 것부터 맞춰 보고, 남은 글자에 다시 적용한다
# ('강남제비스코와는' → 와 + 는). '그룹·계열사·측·우(우선주)' 는 회사를 가리키는 접미라 함께
# 허용한다
_NAME_SUFFIXES = (
    "그룹주", "그룹", "계열사", "측", "우",
    "에서는", "으로는", "으로서", "으로써", "이라고",
    "에서", "에게", "에는", "에도", "와의", "과의", "로는", "로서", "로써",
    "부터", "까지", "보다", "처럼", "이라", "라고", "이나", "이며", "이고", "으로",
    "는", "은", "이", "가", "을", "를", "와", "과", "의", "에", "도", "로",
    "등", "만", "라", "나", "며", "고",
)  # fmt: skip


def split_sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENTENCE_SPLIT.split(text) if s.strip()]


def lead_of(text: str) -> str:
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    return "\n".join(paragraphs[:LEAD_PARAGRAPHS])[:LEAD_CHARS]


def sentences_with(surface: str, sentences: list[str]) -> list[str]:
    """표기가 든 문장을 등장 순서대로 최대 SENTENCES_PER_CANDIDATE 개."""
    return [s for s in sentences if surface in s][:SENTENCES_PER_CANDIDATE]


def tokens_with(surface: str, sentences: list[str]) -> list[str]:
    """표기를 품은 어절을 등장 순서대로, 중복 없이. 조사까지 붙은 그대로 둔다("삼성SDI와").

    표기 자체에 공백이 있을 수 있어("YG PLUS", "LS ELECTRIC") 어절을 공백으로 자르지 않고,
    표기의 등장 위치에서 양쪽으로 경계 문자까지 넓힌다."""
    seen: dict[str, None] = {}
    for sentence in sentences:
        start = sentence.find(surface)
        while start != -1:
            left = start
            while left > 0 and sentence[left - 1] not in _TOKEN_BOUNDARY:
                left -= 1
            right = start + len(surface)
            while right < len(sentence) and sentence[right] not in _TOKEN_BOUNDARY:
                right += 1
            token = sentence[left:right].strip(_TOKEN_STRIP)
            if surface in token:
                seen.setdefault(token, None)
            start = sentence.find(surface, start + 1)
    return list(seen)


def _only_suffixes(rest: str) -> bool:
    if not rest:
        return True
    return any(rest.startswith(s) and _only_suffixes(rest[len(s) :]) for s in _NAME_SUFFIXES)


def token_is_name(surface: str, tokens: list[str]) -> bool:
    """어절 중 하나라도 "표기" 또는 "표기 + 조사·접미"면 True. 전부 다른 긴 단어의 일부면 False.

    'SK하이닉스는'·'디오션자산운용의'·'인플레이션' 은 False, '삼성SDI와'·'SK그룹에서'·
    '강남제비스코와는' 은 True. 표기 앞에 글자가 붙은 어절('소비자' 의 비자)도 False."""
    return any(t.startswith(surface) and _only_suffixes(t[len(surface) :]) for t in tokens)


def build_state(
    title: str, query_companies: list[str], text: str, candidates: list[dict[str, str]]
) -> tuple[dict[str, Any], list[str]]:
    """기사 하나의 jev state 와, 어절 규칙으로 먼저 버린 표기 목록. 순수 함수.

    제목을 첫 문장으로 두고 훑는다 — 후보에는 제목에만 나온 기업도 있다. 리드는 본문만이다.
    state.candidates 에는 어절 규칙을 통과한 후보만 든다(등장 순서 유지). name 은 사전 표준명,
    surface 는 제목·본문 표기 — 둘이 다를 수 있다('HD현대重' / 'HD현대중공업').
    state 의 키(`query_stocks` 등)는 질문이 경로로 가리키므로 v2 와 같은 이름을 쓴다."""
    sentences = [title, *split_sentences(text)] if title else split_sentences(text)
    kept: list[dict[str, Any]] = []
    dropped: list[str] = []
    for candidate in candidates:
        surface = candidate["surface"]
        found = sentences_with(surface, sentences)
        if token_is_name(surface, tokens_with(surface, found)):
            kept.append({"name": candidate["name"], "surface": surface, "sentences": found})
        else:
            dropped.append(surface)
    state = {
        "headline": title,
        "query_stocks": query_companies,
        "lead": lead_of(text),
        "candidates": kept,
    }
    return state, dropped


def decide(answers: dict[str, Any], i: int) -> tuple[Role, dict[str, float]]:
    """후보 i 의 역할과 원시 확률. 역할은 THRESHOLDS 로 정한다."""
    is_company = answers[f"c{i}_is_company"].noul
    role = answers[f"c{i}_role"]
    p_party = role.probabilities.get("party", 0.0)
    scores = {
        "is_company": is_company,
        "p_party": p_party,
        "role_confidence": role.confidence,
        **{f"p_{k}": v for k, v in role.probabilities.items() if k != "party"},
    }
    if is_company < THRESHOLDS["company_min"]:
        return "not_company", scores
    if p_party >= THRESHOLDS["party_min"]:
        return "party", scores
    if role.choice != "party":
        return role.choice, scores
    # party 로 골랐지만 임계값에 못 미쳤다 — 그 다음으로 높은 비-party 역할을 보고한다
    best = max((k for k in role.probabilities if k != "party"), key=role.probabilities.__getitem__)
    return best, scores


class EntityJudge:
    """jev 호출. 클라이언트는 이벤트 루프 안에서 처음 부를 때 만들어 재사용한다."""

    def __init__(self):
        from pipelines.news.transformers.prompts.entity import build_questions

        self._build_questions = build_questions
        self._client = None

    def _get_client(self):
        if self._client is None:
            from pipelines.common.clients.jev import async_client

            self._client = async_client()
        return self._client

    async def judge(
        self,
        title: str,
        query_companies: list[str],
        text: str,
        candidates: list[dict[str, str]],
    ) -> EntityJudgementList:
        state, dropped = build_state(title, query_companies, text, candidates)
        judgements = [EntityJudgement(entity=surface, role="not_company") for surface in dropped]
        if state["candidates"]:
            questions = self._build_questions(len(state["candidates"]), "lead")
            response = await self._get_client().system_one(state, questions)
            for i, candidate in enumerate(state["candidates"]):
                role, scores = decide(response.answers, i)
                judgements.append(
                    EntityJudgement(entity=candidate["surface"], role=role, scores=scores)
                )
        return EntityJudgementList(judgements=judgements)


@lru_cache
def get_entity_judge() -> Judge:
    """프로세스당 하나. 처음 부를 때 typesafe_sdk 를 로드하므로 import 시점엔 비용이 없다."""

    return EntityJudge().judge


async def judge_items(
    items: list[dict[str, Any]], judge: Judge, max_concurrency: int
) -> EntityFilterResult:
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    result = EntityFilterResult()

    async def one(item: dict[str, Any]) -> None:
        companies = item["_candidates"]
        candidates = [{"name": c["name"], "surface": c["surface"]} for c in companies]
        result.judged += 1

        async with semaphore:
            started = time.monotonic()
            try:
                verdict = await judge(
                    item.get("title", ""),
                    # 관련성 필터를 통과한 검색 종목 — 이 기사의 주인공이다
                    [company["name"] for company in item.get("_linked_companies") or []],
                    item.get("_text") or "",
                    candidates,
                )
            except Exception as e:
                result.failed += 1
                logger.warning(
                    "엔티티 판정 호출 실패 (검색 종목만 연결): %s / %s: %s",
                    item.get("link", ""),
                    type(e).__name__,
                    e,
                )
                return
            finally:
                result.latencies_ms.append((time.monotonic() - started) * 1000)

        surfaces = {c["surface"] for c in candidates}
        result.roles.update(
            judgement.role
            for judgement in verdict.judgements
            if judgement.entity.strip() in surfaces
        )
        # 평가가 원시 확률을 읽을 수 있게 판정을 item 에 남긴다 (저장에는 쓰이지 않는다)
        item["_entity_judgements"] = [j.model_dump() for j in verdict.judgements]
        passed = resolve_judgements(companies, verdict.judgements)
        result.kept += len(passed)
        item["_linked_companies"] = merge_linked(item.get("_linked_companies") or [], passed)

    await asyncio.gather(*(one(item) for item in items))
    return result


def filter_candidates(
    items: list[dict[str, Any]],
    judge: Judge | None = None,
    max_concurrency: int | None = None,
) -> EntityFilterResult:
    """제목·본문의 후보 기업(`_candidates`)을 판정해 통과한 기업을 `_linked_companies` 에 더한다.

    judge 에는 제목과 기존 `_linked_companies`(관련성을 통과한 검색 종목)의 이름도 넘긴다 — 이
    함수는 관련성 필터와 match_candidates 뒤에 돌아야 한다.

    `_candidates` 가 빈 기사는 judge 를 부르지 않는다. judge 를 안 주면 jev 싱글톤을 쓴다.
    테스트는 가짜 judge 를 넘긴다. 본문은 자르지 않는다 — state 에는 리드와 표기가 든 문장만
    들어가므로 길이는 거기서 제한된다.
    """

    targets = [item for item in items if item.get("_candidates")]
    if not targets:
        return EntityFilterResult()

    if judge is None:
        judge = get_entity_judge()

    if max_concurrency is None:
        max_concurrency = get_news_settings().news_llm_max_concurrency

    return asyncio.run(judge_items(targets, judge, max_concurrency))
