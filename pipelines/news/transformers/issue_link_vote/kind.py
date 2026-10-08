"""이슈 성격을 event(회사 자신의 사건)와 market_reaction(외부 요인에 따른 주가 반응) 중 하나로
분류한다.

이슈마다 한 번 분류하고, 결과는 job 이 news_clusters 에 저장한다. 입력은 제목, 기사 제목, 한 줄
요약, 요약 문단 순서다. 요약은 대표 기사 하나에서 나와 앞선 사실을 배경으로 다시 적는 일이 많으므로,
판정 근거인 제목과 기사 제목을 앞에 둔다. 형식 재요청 뒤에도 응답을 읽지 못하면 다른 투표자와
마찬가지로 가장 보수적인 답을 고른다. 즉 연결에서 빠지는 market_reaction 으로 분류하고, 사유에 그
사실을 남긴다.

두 모델로 나눠 분류한다. 값싼 1차 모델이 모든 이슈를 먼저 분류하고, 아래 경우에만 성격 분류 모델이
같은 프롬프트로 다시 판정해 그 답을 최종 답으로 쓴다.
  - 1차 답이 market_reaction 이다. 이슈를 타임라인에서 빼는 답이므로 더 강한 모델이 확인한다.
  - 1차 답이 event 이고 이슈 제목이나 기사 제목에 REPORT_CUES 단어가 있다.
그 밖에는 1차 답이 최종 답이다. 1차 모델이 없거나 성격 분류 모델과 같으면 성격 분류 모델
하나로 분류한다.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from pipelines.news.transformers.issue_link_vote.issue import Issue, collapse
from pipelines.news.transformers.issue_link_vote.llm import PROMPT_VERSIONS, LlmClient
from pipelines.news.transformers.prompts.issue_kind import SYSTEM, TOOL

EVENT = "event"
MARKET_REACTION = "market_reaction"
KINDS = (EVENT, MARKET_REACTION)
MAX_TOKENS = 400
MAX_TITLES = 10
UNPARSED_REASON = "분류 응답을 읽지 못해 연결에서 뺀다"
STAGE = "issue_kind"
# 증권사 리포트·전망 이슈는 회사의 앞선 사실을 배경으로 적는 일이 많아 값싼 1차 모델이 event 로
# 읽기 쉽다. 1차 답이 event 라도 이 단어가 이슈 제목이나 기사 제목에 있으면 성격 분류 모델에
# 다시 묻는다.
REPORT_CUES = (
    "목표가",
    "목표주가",
    "투자의견",
    "증권",
    "리포트",
    "전망",
    "기대",
    "수혜",
    "컨센서스",
    "추정",
    "예상",
    "프리뷰",
)


@dataclass(frozen=True)
class IssueKind:
    """최종 성격 분류다. model 은 최종 답을 낸 모델이고, escalated 는 1차 답 뒤에
    성격 분류 모델이 다시 판정했는지를 나타낸다."""

    kind: str
    reason: str
    model: str
    prompt_version: str
    escalated: bool = False


def kind_input(issue: Issue) -> str:
    titles: list[str] = []
    for member in issue.members:
        title = collapse(member.title)
        if title and title not in titles:
            titles.append(title)
    lines = [
        f"제목: {collapse(issue.title)}",
        "기사 제목:",
        *(f"- {t}" for t in titles[:MAX_TITLES]),
    ]
    if not titles:
        lines.append("- (없음)")
    lines += [
        f"한 줄 요약: {collapse(issue.node_summary) or '(없음)'}",
        f"요약: {collapse(issue.summary) or '(없음)'}",
    ]
    return "\n".join(lines)


def has_report_cue(issue: Issue) -> bool:
    titles = [issue.title, *(member.title for member in issue.members)]
    return any(cue in (title or "") for title in titles for cue in REPORT_CUES)


def needs_confirmation(issue: Issue, kind: str) -> bool:
    """1차 답을 성격 분류 모델에 다시 물어야 하는지 정한다."""

    return kind == MARKET_REACTION or (kind == EVENT and has_report_cue(issue))


def classify_kind(issue: Issue, client: LlmClient, screen_model: str = "") -> IssueKind:
    """이슈 성격을 분류한다. screen_model 이 비었거나 성격 분류 모델과 같으면
    성격 분류 모델 하나로 분류한다."""

    if not screen_model or screen_model == client.model_for(STAGE):
        return _ask(issue, client)
    first = _ask(issue, client, screen_model)
    if not needs_confirmation(issue, first.kind):
        return first
    return replace(_ask(issue, client), escalated=True)


def _ask(issue: Issue, client: LlmClient, model: str | None = None) -> IssueKind:
    reply = client.call(
        STAGE,
        system=SYSTEM,
        user=kind_input(issue),
        max_tokens=MAX_TOKENS,
        tool=TOOL,
        cluster_id=issue.id,
        model=model,
    )
    obj = reply.obj or {}
    kind = obj.get("kind")
    if kind in KINDS:
        reason = collapse(obj.get("reason")) or collapse(obj.get("main_news"))
    else:
        kind, reason = MARKET_REACTION, UNPARSED_REASON
    return IssueKind(
        kind=kind,
        reason=reason,
        model=reply.model,
        prompt_version=PROMPT_VERSIONS[STAGE],
    )
