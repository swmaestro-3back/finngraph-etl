"""이슈 타임라인 연결: news_cluster_articles·news_backfill_cluster_articles DAG 의 link_issues
task 이다.

이슈(이름과 대표 기사가 있는 클러스터)를 같은 타임라인의 앞선 이슈에 잇는다. 이을 부모가 없는
이슈는 루트(부모가 없는 타임라인 첫 이슈)가 된다. 노드 제목은 news_clusters.title 을, 한 줄 요약은
summarize_articles 가 대표 기사에 남긴 핵심 포인트를 그대로 쓴다.

판정 방식은 NEWS_ISSUE_LINK_METHOD 로 정한다.
  vote    LLM 투표(transformers/issue_link_vote). 이슈마다 성격(event / market_reaction)을 한 번
          분류해 남기고, 주가 반응 이슈는 부모도 후보도 되지 않는다. 주가 반응 이슈 자신은 루트로
          판정한다. 분류 응답을 읽지 못한 이슈는 성격을 저장하지 않는다. 그런 대상은 실패로 세고,
          그런 후보는 이번 실행의 후보에서만 뺀다. 후보는 후보 범위 규칙(universe.py)을 통과한 앞선
          event 이슈이고, 제안자(pair·rank)가 받아들인 쌍만 확인자(judge·check)에게 묻는다. 후보별
          표는 news_issue_link_votes, LLM 응답은 news_issue_link_llm_calls 에 남는다(같은 입력이면
          다음 실행이 저장된 응답을 다시 쓴다).
  cosine  주요 기업 겹침과 코사인 임계값만 보는 이전 규칙이다. LLM 을 부르지 않으며, 되돌릴 때
          쓴다.

1. linked_at 이 NULL 인 이슈를 오래된 순으로 고른다(지금부터 NEWS_ISSUE_LINK_LOOKBACK_DAYS 안,
   실행당 NEWS_ISSUE_LINK_MAX_PER_RUN 개). 대표 기사 요약이 아직 없는 이슈는 마지막 갱신 뒤
   SUMMARY_WAIT 이 지나야 요약 없이 고른다.
2. 임베딩이 없는 대상은 제목, 한 줄 요약, 멤버 기사 제목 몇 개를 이은 텍스트로 한 번에 임베딩해
   저장한다. 기사 제목은 후보 기사(승격 전에 들어온 기사)부터 넣는다.
3. 재판정 대상을 고른다. 재판정 대상은 이번 대상 중 가장 먼저 시작한 이슈보다 뒤에 시작했고 이미
   판정된 이슈이며, 지금부터 NEWS_ISSUE_RELINK_WINDOW_HOURS 안에 시작한 것만 고른다. 대상은 시작
   순서가 아니라 이름·대표 기사·요약을 받는 순서로 들어온다. 그래서 먼저 시작한 이슈가 늦게
   들어오면 재판정 대상은 그 이슈를 부모로 보지 못한 채 판정돼 있다(같은 사건이 둘로 나뉜
   클러스터가 각각 루트로 남는다). 이들을 다시 판정하면 재판정 기간 안에서는 시작 순서대로 한 번에
   이은 결과와 같아진다.
4. 대상과 재판정 대상을 오래된 순으로 하나씩 잇는다. 판정마다 커밋하므로 같은 실행에서 뒤에 오는
   이슈는 앞선 이슈를 부모 후보로 본다.

클러스터 하나가 실패하면(LLM 오류 포함) 세고 넘어간다. 실패한 대상은 linked_at 이 NULL 로 남아
다음 실행에서 다시 고르고, 재판정에 실패한 이슈는 지난 판정을 유지한다. 전부 실패하면 Bedrock·DB
장애로 보고 예외를 올려 Airflow 가 재시도하게 한다.
투표 판정의 LLM 호출 상한(실행당·이슈당)에 닿은 이슈는 실패가 아니라 미룸(deferred)으로 센다. 받은
응답은 캐시에 남아 다음 실행이 이어서 진행한다. 실행당 상한에 닿으면 남은 대상도 모두 다음 실행으로
미룬다. 미룬 재판정 대상은 지난 판정을 둔 채 linked_at 만 비워 다음 실행의 대상으로 돌린다. 그대로
두면 다음 실행이 더 늦게 시작한 대상을 기준으로 재판정 대상을 고를 때 그 이슈를 다시 판정하지
못한다.

스케줄 진입점 run() 은 NEWS_ISSUE_LINK_ENABLED 설정이 켜져 있을 때만 실행된다. 재판정 기간 밖의
판정은 다시 만들지 않으므로, 연결 백필 전에 켜면 최근 이슈가 lookback 밖의 옛 이슈를 부모로 보지
못한 채 루트로 남는다. 같은 이유로 옛 기사를 한꺼번에 들이는 백필(news_backfill_krx100) 뒤에는 그
기간의 연결을 다시 만든다(pipelines/news/README.md "이슈 타임라인"). 백필 DAG
(news_backfill_issue_timeline)는 활성화 설정을 거치지 않고 link_pending() 을 직접 부르며, 재판정
대상을 고르는 범위를 백필 기간 전체로 넓힌다. 실패하거나 미룬 옛 대상이 뒤 배치에서 판정되면 그보다
뒤에 시작한 판정을 모두 다시 만들어야 시작 순서로 한 번에 이은 결과와 같아지기 때문이다.
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

from pipelines.common.clients.bedrock import embed_texts, get_bedrock_client
from pipelines.common.config import get_settings
from pipelines.common.gazetteer import get_company_matcher
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.news.config import NewsSettings, get_news_settings
from pipelines.news.repositories.postgres.issue_links import (
    IssueRow,
    LinkVoteRow,
    PostgresCallStore,
    fetch_company_names,
    fetch_issue_rows,
    fetch_vote_candidates,
    save_issue_kind,
    save_link_votes,
)
from pipelines.news.repositories.postgres.news_clusters import (
    LinkTarget,
    fetch_cluster_members,
    fetch_link_candidates,
    fetch_link_targets,
    fetch_relink_tail,
    record_link_decision,
    release_link_decisions,
    save_cluster_embedding,
)
from pipelines.news.transformers.issue_link_vote import universe
from pipelines.news.transformers.issue_link_vote.decide import CandidateVotes
from pipelines.news.transformers.issue_link_vote.issue import CompanyName, Issue, Member
from pipelines.news.transformers.issue_link_vote.kind import (
    EVENT,
    MARKET_REACTION,
    IssueKind,
    classify_kind,
)
from pipelines.news.transformers.issue_link_vote.linker import RelationRule, VoteLinker
from pipelines.news.transformers.issue_link_vote.llm import (
    ISSUE_KIND_SCREEN,
    STAGES,
    BudgetExceeded,
    Invoker,
    LlmClient,
    RunBudgetExceeded,
    parallel_map,
)
from pipelines.news.transformers.issue_link_vote.views import EventScorer
from pipelines.news.transformers.issue_linker import (
    ClusterMember,
    LinkDecision,
    TitleMatcher,
    build_embedding_text,
    decide_link,
    node_summary,
    primary_company_ids,
    to_vector_literal,
)

logger = get_logger(__name__)

# news_clusters.embedding 컬럼이 vector(1024) 로 고정돼 있어서 설정이 아니라 코드 상수로 둔다.
ISSUE_EMBEDDING_DIM = 1024
# 부모 후보를 한 번에 읽는 수다. SQL 은 대상의 주요 기업이 연결된 후보까지만 거르고, 후보 쪽의 주요
# 기업 규칙은 파이썬에서 다시 거른다. 그 기업이 본문에만 언급된 후보가 페이지를 다 채우면 맞는
# 부모가 뒤에 있어도 잘리므로, 맞는 후보가 나오거나 후보가 더 없을 때까지 다음 페이지를 읽는다.
CANDIDATE_PAGE_SIZE = 30
# 대표 기사 요약이 없는 이슈를 요약 없이 잇기 전에 기다리는 시간이다. 클러스터 updated_at(승격·제목
# 생성 때 갱신)부터 재며, 그동안 클러스터 DAG 가 돌 때마다 요약을 다시 시도한다.
SUMMARY_WAIT = timedelta(hours=24)

# 투표 판정 후보를 한 번에 읽는 상한이다(코사인 내림차순). pair 는 코사인 상위 12개를, rank 는
# 주요 기업이 겹치는 후보부터 10개를 이 안에서 고른다.
VOTE_CANDIDATE_LIMIT = 300

# relinked 는 재판정한 이슈 수, relink_changed 는 그중 부모가 바뀐 수다. linked·roots 는 이번
# 대상만 센다. 아래 투표 판정 통계에서 classified 는 이번 실행에서 새로 분류한 이슈 수(후보 포함),
# market_reaction 은 그중 주가 반응으로 분류돼 연결에서 빠진 수, kind_escalated 는 그중 1차
# 모델의 답을 성격 분류 모델이 다시 판정한 수, kind_unknown 은 분류 응답을 읽지 못해 저장하지 않은
# 이슈 수(대상은 실패로도 센다), proposed·confirmed 는 제안자가 받아들인 쌍과 확인자까지 통과한
# 쌍의 수, deferred 는 호출 상한으로 다음 실행에 미룬 대상·재판정 대상 수,
# llm_calls 는 새로 부른 LLM 호출 수, cache_hits 는 캐시에서 읽은 응답 수, llm_cost_usd 는 그 호출의
# 추정 비용(성격 분류의 두 모델 포함)이다.
STAT_KEYS = (
    "scanned",
    "embedded",
    "linked",
    "same_event",
    "follow_up",
    "roots",
    "relinked",
    "relink_changed",
    "failed",
    "classified",
    "market_reaction",
    "kind_escalated",
    "kind_unknown",
    "proposed",
    "confirmed",
    "deferred",
    "llm_calls",
    "cache_hits",
    "llm_cost_usd",
)


def _embed_missing(
    targets: list[LinkTarget], members: dict[int, list[ClusterMember]], model: str
) -> tuple[dict[int, str], set[int]]:
    """임베딩이 없는 대상을 한 번에 임베딩해 저장하고 (저장된 id → 벡터 텍스트, 실패한 id)를
    돌려준다."""

    texts = [
        build_embedding_text(
            target.title,
            node_summary(target.summary_points, target.summary),
            [member.title for member in members.get(target.cluster_id, [])],
        )
        for target in targets
    ]

    try:
        vectors = embed_texts(texts, dim=ISSUE_EMBEDDING_DIM, model=model)
    except Exception as e:
        logger.warning(
            "[link_issues] 임베딩 실패(다음 런에 재시도): %d건, %s: %s",
            len(targets),
            type(e).__name__,
            e,
        )
        return {}, {target.cluster_id for target in targets}

    saved: dict[int, str] = {}
    failed: set[int] = set()
    for target, vector in zip(targets, vectors, strict=True):
        literal = to_vector_literal(vector)
        try:
            save_cluster_embedding(target.cluster_id, literal)
        except Exception as e:
            failed.add(target.cluster_id)
            logger.warning(
                "[link_issues] 임베딩 저장 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                target.cluster_id,
                type(e).__name__,
                e,
            )
            continue
        saved[target.cluster_id] = literal

    return saved, failed


class _PrimaryCompanies:
    """클러스터별 주요 기업을 실행 동안 캐시한다. 같은 부모 후보가 여러 대상에서 반복해 나오기
    때문이다."""

    def __init__(self, matcher: TitleMatcher, members: dict[int, list[ClusterMember]]):
        self._matcher = matcher
        self._members = members
        self._cache: dict[int, frozenset[int]] = {}

    def get(self, cluster_ids: list[int]) -> dict[int, frozenset[int]]:
        missing = [
            cluster_id
            for cluster_id in cluster_ids
            if cluster_id not in self._cache and cluster_id not in self._members
        ]
        if missing:
            self._members.update(fetch_cluster_members(missing))

        for cluster_id in cluster_ids:
            if cluster_id not in self._cache:
                self._cache[cluster_id] = primary_company_ids(
                    self._members.get(cluster_id, []), self._matcher
                )
        return {cluster_id: self._cache[cluster_id] for cluster_id in cluster_ids}


def _choose_parent(
    target: LinkTarget,
    embedding: str,
    primary: _PrimaryCompanies,
    settings: NewsSettings,
) -> LinkDecision:
    """부모 후보를 코사인 내림차순 페이지로 읽어 처음 나오는 맞는 후보를 부모로 고른다.

    SQL 정렬이 choose_parent 의 우선순위와 같으므로, 페이지에서 처음 맞는 후보가 전체에서 가장
    좋은 부모다.
    """

    company_ids = primary.get([target.cluster_id])[target.cluster_id]
    window_start = target.first_published_at - timedelta(days=settings.issue_link_lookback_days)
    min_score = (
        settings.issue_link_threshold if company_ids else settings.issue_link_no_company_threshold
    )
    after = None
    while True:
        page = fetch_link_candidates(
            cluster_id=target.cluster_id,
            embedding=embedding,
            first_published_at=target.first_published_at,
            company_ids=company_ids,
            window_start=window_start,
            min_score=min_score,
            limit=CANDIDATE_PAGE_SIZE,
            after=after,
        )
        candidate_companies = primary.get([candidate.cluster_id for candidate in page])
        decision = decide_link(
            [
                replace(candidate, company_ids=candidate_companies[candidate.cluster_id])
                for candidate in page
            ],
            cluster_id=target.cluster_id,
            first_published_at=target.first_published_at,
            company_ids=company_ids,
            threshold=settings.issue_link_threshold,
            no_company_threshold=settings.issue_link_no_company_threshold,
            same_event_max_gap=timedelta(hours=settings.issue_same_event_max_gap_hours),
            same_event_score=settings.issue_same_event_score,
            same_event_score_max_gap=timedelta(hours=settings.issue_same_event_score_max_gap_hours),
        )
        if decision.parent_id is not None or len(page) < CANDIDATE_PAGE_SIZE:
            return decision
        after = page[-1]


def _link_one(
    target: LinkTarget,
    embedding: str,
    primary: _PrimaryCompanies,
    settings: NewsSettings,
    *,
    relink: bool = False,
) -> LinkDecision | None:
    """부모를 골라 기록한다. 쓴 행이 없으면(다른 실행이 먼저 판정했거나 그사이 초기화됐으면)
    None 을 돌려준다."""

    decision = _choose_parent(target, embedding, primary, settings)
    if not record_link_decision(
        target.cluster_id, decision.parent_id, decision.score, decision.relation, relink=relink
    ):
        logger.info(
            "[link_issues] %s(건너뜀): cluster_id=%s",
            "초기화됨" if relink else "이미 판정됨",
            target.cluster_id,
        )
        return None

    if relink:
        if decision.parent_id != target.parent_id:
            logger.info(
                "[link_issues] 재판정 %s '%s': 부모 %s → %s (%s)",
                target.cluster_id,
                target.title,
                target.parent_id,
                decision.parent_id,
                decision.relation,
            )
    elif decision.parent_id is not None:
        logger.info(
            "[link_issues] %s '%s' ← 부모 %s (%s, 코사인 %.3f)",
            target.cluster_id,
            target.title,
            decision.parent_id,
            decision.relation,
            decision.score,
        )
    return decision


def _relink_tail(
    targets: list[LinkTarget],
    now: datetime,
    settings: NewsSettings,
    relink_since: datetime | None = None,
) -> list[LinkTarget]:
    """이번 대상(오래된 순) 중 가장 먼저 시작한 이슈보다 뒤에 시작했고 이미 판정된 이슈를 재판정
    대상으로 고른다. 범위는 relink_since 이후에 시작한 이슈이고, relink_since 를 주지 않으면
    지금부터 NEWS_ISSUE_RELINK_WINDOW_HOURS 안(재판정 기간)이다."""

    if not targets:
        return []
    if relink_since is None:
        if settings.issue_relink_window_hours <= 0:
            return []
        relink_since = now - timedelta(hours=settings.issue_relink_window_hours)
    first = targets[0]
    return fetch_relink_tail(first.first_published_at, first.cluster_id, relink_since)


# ── 투표 판정 ──────────────────────────────────────────────────────────────


def _bedrock_invoker(workers: int) -> Invoker:
    common = get_settings()
    client = get_bedrock_client(
        common.bedrock_region,
        common.bedrock_request_timeout,
        max_pool_connections=max(10, workers * 2),
    )
    return lambda request: client.converse(**request)


def _stage_models() -> dict[str, str]:
    common = get_settings()
    return {stage: common.chat_model(role) for stage, role in STAGES.items()}


def _vote_row(votes: CandidateVotes) -> LinkVoteRow:
    c = votes.confirmation
    return LinkVoteRow(
        candidate_cluster_id=votes.candidate_id,
        cosine=round(votes.cosine, 6),
        event_score=None if votes.event is None else round(votes.event, 6),
        gap_hours=round(votes.gap_hours, 2),
        pair_vote=votes.pair_pass,
        rank_vote=votes.rank_pass,
        judge_vote=votes.judge_pass,
        check_vote=votes.check_pass,
        pair_route=None if votes.pair is None else votes.pair["route"],
        judge_by=None if c is None else c.judge_by,
        pair_label=None if votes.pair is None else votes.pair["label"],
        rank_label=None if votes.rank is None else votes.rank["label"],
        judge_label=None if c is None else c.judge_label,
        judge_a_scope=None if c is None else c.a_scope,
        judge_b_scope=None if c is None else c.b_scope,
        check_role=None if c is None else c.check_role,
        vote_count=votes.vote_count,
        accepted=votes.accepted,
        chosen=votes.chosen,
        evidence=votes.evidence,
    )


class _VoteRun:
    """투표 판정 실행 한 번을 맡는다. 이슈·기업 이름 정보·주요 기업을 실행 동안 메모리에 두고,
    대상을 하나씩 판정해 기록한다."""

    def __init__(
        self,
        settings: NewsSettings,
        members: dict[int, list[ClusterMember]],
        primary: _PrimaryCompanies,
        stats: dict[str, Any],
        run_cap: int | None = None,
    ):
        workers = max(1, settings.news_llm_max_concurrency)
        self.settings = settings
        self.members = members
        self.primary = primary
        self.stats = stats
        self.run_id = uuid.uuid4()
        self.workers = workers
        self.client = LlmClient(
            _stage_models(),
            PostgresCallStore(),
            _bedrock_invoker(workers),
            run_cap=settings.issue_link_llm_max_calls_per_run if run_cap is None else run_cap,
            issue_cap=settings.issue_link_llm_max_calls_per_issue,
        )
        self.names: dict[int, CompanyName] = {}
        self.rows: dict[int, IssueRow] = {}
        self.issues: dict[int, Issue] = {}
        self.kinds: dict[int, str | None] = {}
        # 이번 실행에서 분류 응답을 읽지 못한 이슈다. 저장하지 않고 다음 실행에서 다시 분류한다.
        self.unknown: set[int] = set()
        self.kind_screen_model = get_settings().chat_model(ISSUE_KIND_SCREEN)
        scorer = EventScorer(
            lambda texts: embed_texts(
                texts, dim=ISSUE_EMBEDDING_DIM, model=settings.issue_embedding_model
            ),
            self.names,
        )
        self.linker = VoteLinker(
            self.client,
            self.names,
            scorer,
            RelationRule(
                same_event_max_gap_hours=settings.issue_same_event_max_gap_hours,
                same_event_score=settings.issue_same_event_score,
                same_event_score_max_gap_hours=settings.issue_same_event_score_max_gap_hours,
            ),
            workers,
        )

    # ── 이슈 ──
    def load(self, cluster_ids: list[int]) -> None:
        """이슈 상세·멤버·주요 기업·기업 이름 정보를 한 번에 읽어 Issue 로 만든다."""

        missing = [cid for cid in cluster_ids if cid not in self.issues]
        if not missing:
            return
        self.rows.update(fetch_issue_rows([cid for cid in missing if cid not in self.rows]))
        primary = self.primary.get(missing)
        linked = {
            company_id
            for cid in missing
            for member in self.members.get(cid, [])
            for company_id in member.company_ids
        }
        unnamed = sorted(company_id for company_id in linked if company_id not in self.names)
        self.names.update(fetch_company_names(unnamed))
        for cid in missing:
            row = self.rows.get(cid)
            if row is None:
                continue
            self.kinds.setdefault(cid, row.issue_kind)
            self.issues[cid] = self._issue(row, self.members.get(cid, []), primary[cid])

    def _issue(self, row: IssueRow, members: list[ClusterMember], primary: frozenset[int]) -> Issue:
        linked = sorted({c for member in members for c in member.company_ids})
        return Issue(
            id=row.cluster_id,
            title=row.title,
            first_published_at=row.first_published_at,
            last_published_at=row.last_published_at,
            members=tuple(
                Member(
                    title=member.title,
                    published_at=member.published_at or row.first_published_at,
                    company_ids=member.company_ids,
                )
                for member in members
            ),
            companies={c: self.names[c].name if c in self.names else "" for c in linked},
            primary_company_ids=tuple(sorted(primary)),
            node_summary=node_summary(row.summary_points, row.summary),
            summary=row.summary,
        )

    def kind(self, cluster_id: int) -> str:
        """이슈 성격을 돌려준다. 분류 전이면 분류해 저장하고, 분류하지 못하면 예외를 던진다."""

        known = self.kinds.get(cluster_id)
        if known is not None:
            return known
        result = self._classify(cluster_id)
        if result is None:
            self._mark_unknown(cluster_id)
            raise RuntimeError("성격 분류 응답을 읽지 못함")
        self._store_kind(cluster_id, result)
        return self.kinds[cluster_id]

    def _classify(self, cluster_id: int) -> IssueKind | None:
        return classify_kind(self.issues[cluster_id], self.client, self.kind_screen_model)

    def _mark_unknown(self, cluster_id: int) -> None:
        if cluster_id in self.unknown:
            return
        self.unknown.add(cluster_id)
        self.stats["kind_unknown"] += 1
        logger.warning(
            "[link_issues] 성격 분류 응답을 읽지 못함(저장하지 않고 다음 런에 재시도): %s '%s'",
            cluster_id,
            self.issues[cluster_id].title,
        )

    def _store_kind(self, cluster_id: int, result: IssueKind) -> None:
        save_issue_kind(cluster_id, result.kind, result.reason, result.model, result.prompt_version)
        self.kinds[cluster_id] = result.kind
        self.stats["classified"] += 1
        if result.escalated:
            self.stats["kind_escalated"] += 1
        if result.kind == MARKET_REACTION:
            self.stats["market_reaction"] += 1
            logger.info(
                "[link_issues] 주가 반응 이슈로 분류(연결에서 뺌): %s '%s' — %s",
                cluster_id,
                self.issues[cluster_id].title,
                result.reason,
            )

    # ── 후보 ──
    def candidates(self, target: LinkTarget, embedding: str) -> list[universe.Candidate]:
        """후보 범위 규칙을 통과한 앞선 event 이슈를 고른다. 분류 전인 후보는 여기서 분류하고,
        분류하지 못한 후보는 이번 실행의 후보에서만 뺀다."""

        child = self.issues[target.cluster_id]
        rows = fetch_vote_candidates(
            cluster_id=target.cluster_id,
            embedding=embedding,
            first_published_at=target.first_published_at,
            company_ids=child.linked,
            window_start=target.first_published_at
            - timedelta(days=self.settings.issue_link_lookback_days),
            shared_min=universe.SHARED_MIN_COSINE,
            no_share_min=universe.NO_SHARE_MIN_COSINE,
            limit=VOTE_CANDIDATE_LIMIT,
        )
        ids = [row.cluster_id for row in rows]
        missing = [cid for cid in ids if cid not in self.members]
        if missing:
            self.members.update(fetch_cluster_members(missing))
        self.load(ids)
        for row in rows:
            self.kinds.setdefault(row.cluster_id, row.issue_kind)

        unclassified = [
            cid
            for cid in ids
            if cid in self.issues and self.kinds.get(cid) is None and cid not in self.unknown
        ]
        results = parallel_map(self._classify, unclassified, self.workers)
        for cid, result in zip(unclassified, results, strict=True):
            if result is None:
                self._mark_unknown(cid)
            else:
                self._store_kind(cid, result)

        window = timedelta(days=self.settings.issue_link_lookback_days)
        out = []
        for row in rows:
            parent = self.issues.get(row.cluster_id)
            if parent is None or self.kinds.get(row.cluster_id) != EVENT:
                continue
            if universe.eligible(parent, child, row.score, window):
                out.append(universe.Candidate(parent, row.score))
        return out

    # ── 판정 ──
    def link(self, target: LinkTarget, embedding: str, relink: bool) -> LinkDecision | None:
        """성격 분류, 후보 선정, 투표, 기록 순서로 대상 하나를 판정한다. 그사이 다른 실행이 먼저
        판정했거나 판정이 초기화돼 쓴 행이 없으면 None 을 돌려준다."""

        self.client.begin_issue()
        self.load([target.cluster_id])
        if target.cluster_id not in self.issues:
            raise RuntimeError(f"클러스터 없음: {target.cluster_id}")

        votes: list[CandidateVotes] = []
        if self.kind(target.cluster_id) == MARKET_REACTION:
            decision = LinkDecision()
        else:
            result = self.linker.judge(
                self.issues[target.cluster_id], self.candidates(target, embedding)
            )
            votes = result.votes
            d = result.decision
            decision = LinkDecision(parent_id=d.parent_id, score=d.score, relation=d.relation)

        if not record_link_decision(
            target.cluster_id, decision.parent_id, decision.score, decision.relation, relink=relink
        ):
            logger.info(
                "[link_issues] %s(건너뜀): cluster_id=%s",
                "초기화됨" if relink else "이미 판정됨",
                target.cluster_id,
            )
            return None

        self.stats["proposed"] += sum(1 for v in votes if v.proposed)
        self.stats["confirmed"] += sum(1 for v in votes if v.accepted)
        try:
            save_link_votes(self.run_id, target.cluster_id, [_vote_row(v) for v in votes])
        except Exception as e:
            logger.warning(
                "[link_issues] 투표 기록 실패(판정은 남음): cluster_id=%s, %s: %s",
                target.cluster_id,
                type(e).__name__,
                e,
            )

        if decision.parent_id is not None and (
            not relink or decision.parent_id != target.parent_id
        ):
            logger.info(
                "[link_issues] %s%s '%s' ← 부모 %s (%s, s=%.3f)",
                "재판정 " if relink else "",
                target.cluster_id,
                self.issues[target.cluster_id].title,
                decision.parent_id,
                decision.relation,
                decision.score,
            )
        return decision

    def finish(self) -> None:
        call_stats = self.client.stats
        self.stats["llm_calls"] = call_stats.calls
        self.stats["cache_hits"] = call_stats.cache_hits
        self.stats["llm_cost_usd"] = round(call_stats.cost_usd, 4)
        if self.client.unpriced:
            logger.warning(
                "[link_issues] 단가표에 없는 모델은 비용 0 으로 셌다: %s",
                sorted(self.client.unpriced),
            )
        logger.info(
            "[link_issues] LLM 새 호출 %d(단계별 %s), 캐시 %d, 읽지 못한 응답 %d"
            "(캐시에서 지움 %d), 추정 비용 $%.4f",
            call_stats.calls,
            call_stats.by_stage,
            call_stats.cache_hits,
            call_stats.parse_failures,
            call_stats.evicted,
            call_stats.cost_usd,
        )


def run() -> dict[str, Any]:
    """스케줄 진입점으로, 지금부터 lookback 안의 연결 대상을 실행당 상한만큼 잇는다.

    NEWS_ISSUE_LINK_ENABLED 가 꺼져 있으면 아무것도 읽지 않고 모든 값이 0 인 통계를 돌려준다.
    """

    settings = get_news_settings()
    if not settings.issue_link_enabled:
        logger.info("[link_issues] NEWS_ISSUE_LINK_ENABLED=false — 건너뜀")
        return dict.fromkeys(STAT_KEYS, 0)

    return link_pending(
        since=now_kst() - timedelta(days=settings.issue_link_lookback_days),
        limit=settings.issue_link_max_per_run,
    )


def link_pending(
    since: datetime,
    limit: int,
    *,
    relink_since: datetime | None = None,
    llm_call_cap: int | None = None,
) -> dict[str, Any]:
    """first_published_at 이 since 이후인 연결 대상을 limit 개까지 임베딩하고, 재판정 대상과 함께
    오래된 순으로 잇는다.

    NEWS_ISSUE_LINK_ENABLED 와 무관하게 실행되며, 백필 DAG 는 기간을 넓혀 부른다. 통계의
    linked 는 same_event 와 follow_up 의 합이고, 다른 실행이 먼저 판정한 대상은 어느 항목에도 세지
    않는다.

    relink_since 를 주면 그 시각 이후에 시작해 이미 판정된 이슈를 모두 재판정 대상으로 고른다(주지
    않으면 지금부터 NEWS_ISSUE_RELINK_WINDOW_HOURS 안). llm_call_cap 은 이번 실행에서 투표 판정이
    새로 부를 LLM 호출 상한이며, NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN 보다 작을 때만 그 값을
    대신한다.
    """

    settings = get_news_settings()
    stats: dict[str, Any] = dict.fromkeys(STAT_KEYS, 0)
    now = now_kst()
    targets = fetch_link_targets(since, limit, now - SUMMARY_WAIT)
    stats["scanned"] = len(targets)
    if not targets:
        logger.info("[link_issues] 대상 없음")
        return stats

    members = fetch_cluster_members([target.cluster_id for target in targets])
    embeddings = {target.cluster_id: target.embedding for target in targets if target.embedding}
    failed: set[int] = set()

    missing = [target for target in targets if not target.embedding]
    if missing:
        saved, embed_failed = _embed_missing(missing, members, settings.issue_embedding_model)
        embeddings.update(saved)
        failed |= embed_failed
        stats["embedded"] = len(saved)

    # 재판정 대상은 이번에 실제로 이을 대상(임베딩이 있는 것)을 기준으로 고른다. 임베딩에 실패한
    # 대상은 다음 실행에 다시 들어올 때 기준이 된다.
    linkable = [target for target in targets if target.cluster_id in embeddings]
    tail = _relink_tail(linkable, now, settings, relink_since)
    if tail:
        members.update(fetch_cluster_members([target.cluster_id for target in tail]))
        embeddings.update({target.cluster_id: target.embedding for target in tail})
        logger.info(
            "[link_issues] 꼬리 재판정 %d개 (%s 뒤에 시작)",
            len(tail),
            linkable[0].first_published_at,
        )

    queue = sorted(
        [(target, False) for target in linkable] + [(target, True) for target in tail],
        key=lambda item: (item[0].first_published_at, item[0].cluster_id),
    )
    primary = _PrimaryCompanies(get_company_matcher(), members)
    run_cap = settings.issue_link_llm_max_calls_per_run
    if llm_call_cap is not None:
        run_cap = min(run_cap, llm_call_cap)
    vote_run = (
        _VoteRun(settings, members, primary, stats, run_cap)
        if settings.issue_link_method == "vote"
        else None
    )
    # 호출 상한 때문에 미룬 재판정 대상이다. 실행이 끝나면 판정 전으로 돌려 다음 실행의 대상이
    # 되게 한다.
    released: list[int] = []
    for index, (target, relink) in enumerate(queue):
        try:
            if vote_run is None:
                decision = _link_one(
                    target, embeddings[target.cluster_id], primary, settings, relink=relink
                )
            else:
                decision = vote_run.link(target, embeddings[target.cluster_id], relink)
        except RunBudgetExceeded as e:
            stats["deferred"] += len(queue) - index
            released += [item.cluster_id for item, is_tail in queue[index:] if is_tail]
            logger.warning("[link_issues] %s — 남은 %d개는 다음 런에", e, len(queue) - index)
            break
        except BudgetExceeded as e:
            stats["deferred"] += 1
            if relink:
                released.append(target.cluster_id)
            logger.warning("[link_issues] %s — cluster_id=%s 는 다음 런에", e, target.cluster_id)
            continue
        except Exception as e:
            failed.add(target.cluster_id)
            logger.warning(
                "[link_issues] 연결 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                target.cluster_id,
                type(e).__name__,
                e,
            )
            continue

        if decision is None:
            continue
        if relink:
            stats["relinked"] += 1
            stats["relink_changed"] += decision.parent_id != target.parent_id
        elif decision.parent_id is None:
            stats["roots"] += 1
        else:
            stats["linked"] += 1
            stats[decision.relation] += 1

    if released:
        try:
            release_link_decisions(released)
            logger.info("[link_issues] 미룬 꼬리 %d개를 다음 런의 대상으로 돌림", len(released))
        except Exception as e:
            logger.warning(
                "[link_issues] 미룬 꼬리를 대상으로 돌리지 못함(지난 판정이 남음): %d개, %s: %s",
                len(released),
                type(e).__name__,
                e,
            )

    stats["failed"] = len(failed)
    if vote_run is not None:
        vote_run.finish()
    logger.info(
        "[link_issues] (%s) 대상 %d → 임베딩 %d, 연결 %d(같은 사건 %d / 후속 %d), 루트 %d, "
        "꼬리 재판정 %d(부모 바뀜 %d), 실패 %d, 미룸 %d, 분류 %d(주가 반응 %d, 재판정 %d), "
        "분류 못 함 %d, 제안 %d / 확인 %d",
        settings.issue_link_method,
        stats["scanned"],
        stats["embedded"],
        stats["linked"],
        stats["same_event"],
        stats["follow_up"],
        stats["roots"],
        stats["relinked"],
        stats["relink_changed"],
        stats["failed"],
        stats["deferred"],
        stats["classified"],
        stats["market_reaction"],
        stats["kind_escalated"],
        stats["kind_unknown"],
        stats["proposed"],
        stats["confirmed"],
    )

    if stats["failed"] == stats["scanned"] + len(tail):
        raise RuntimeError(
            f"이슈 연결 전건 실패 ({stats['failed']}건) — Bedrock·DB 장애로 보고 재시도한다"
        )
    return stats
