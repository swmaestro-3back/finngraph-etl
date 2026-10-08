"""link_issues job 의 투표 판정 흐름 테스트다. DB·Bedrock·개체 사전은 메모리 가짜로 바꾼다.

가짜 저장소는 SQL 과 같은 규칙(판정 끝남, 먼저 시작, lookback, 주가 반응 제외, 코사인과 연결 기업
하한)으로 후보를 고르고 판정·분류·투표를 바로 반영한다.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest
from issue_link_vote_fakes import FakeBedrock, MemoryStore, PairScript

from pipelines.common.gazetteer import CompanyMatcher, GazetteerEntry
from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.postgres.issue_links import CandidateRow, IssueRow
from pipelines.news.repositories.postgres.news_clusters import LinkTarget
from pipelines.news.transformers.issue_link_vote.issue import CompanyName
from pipelines.news.transformers.issue_linker import ClusterMember
from pipelines.news.transformers.prompts import issue_kind as kind_prompt

GABIA = 3
MATCHER = CompanyMatcher({"가비아": GazetteerEntry(GABIA, 1, "079940", "가비아")})
VECTOR = "[1.0,0.0]"

PROPOSAL = "맥쿼리 가비아 공개매수 추진"
REACTION = "가비아 중동 해협 봉쇄 우려 급등"
FAILURE = "맥쿼리 가비아 공개매수 실패"


@dataclass
class FakeCluster:
    title: str
    first_published_at: datetime
    embedding: str | None = VECTOR
    linked: bool = False
    parent: int | None = None
    root: int | None = None
    score: float | None = None
    relation: str | None = None
    kind: str | None = None
    kind_reason: str | None = None
    members: list[ClusterMember] = field(default_factory=list)


def _cosine(a: str, b: str) -> float:
    x, y = json.loads(a), json.loads(b)
    dot = sum(p * q for p, q in zip(x, y, strict=True))
    return dot / (math.sqrt(sum(p * p for p in x)) * math.sqrt(sum(q * q for q in y)))


KIMI, SONNET = "moonshotai.kimi-k2.5", "us.anthropic.claude-sonnet-4-6"


@pytest.fixture
def vote(monkeypatch):
    from pipelines.common import config as common_config
    from pipelines.news import config
    from pipelines.news.jobs import link_issues as job

    for name in (
        "BEDROCK_ISSUE_LINK_PROPOSER_MODEL",
        "BEDROCK_ISSUE_LINK_CONFIRMER_MODEL",
        "BEDROCK_ISSUE_KIND_MODEL",
        "BEDROCK_ISSUE_KIND_SCREEN_MODEL",
    ):
        monkeypatch.delenv(name, raising=False)
    common_config.get_settings.cache_clear()
    monkeypatch.setenv("NEWS_ISSUE_LINK_METHOD", "vote")
    monkeypatch.setenv("NEWS_ISSUE_RELINK_WINDOW_HOURS", "0")
    monkeypatch.setenv("NEWS_LLM_MAX_CONCURRENCY", "4")
    config.get_news_settings.cache_clear()

    clusters: dict[int, FakeCluster] = {}
    store = MemoryStore()
    bedrock = FakeBedrock()
    saved: dict = {"votes": [], "kinds": [], "released": []}

    def fetch_link_targets(since, limit, summary_deadline):
        rows = sorted(
            ((cid, c) for cid, c in clusters.items() if not c.linked),
            key=lambda pair: (pair[1].first_published_at, pair[0]),
        )[:limit]
        return [
            LinkTarget(cid, c.title, c.first_published_at, embedding=c.embedding) for cid, c in rows
        ]

    def fetch_cluster_members(cluster_ids):
        return {cid: list(clusters[cid].members) for cid in cluster_ids if clusters[cid].members}

    def fetch_issue_rows(cluster_ids):
        return {
            cid: IssueRow(
                cid,
                clusters[cid].title,
                clusters[cid].first_published_at,
                clusters[cid].first_published_at,
                clusters[cid].kind,
                summary=f"{clusters[cid].title} 요약이에요.",
            )
            for cid in cluster_ids
            if cid in clusters
        }

    def fetch_company_names(company_ids):
        return {c: CompanyName("가비아", "079940") for c in company_ids if c == GABIA}

    def fetch_vote_candidates(
        *,
        cluster_id,
        embedding,
        first_published_at,
        company_ids,
        window_start,
        shared_min,
        no_share_min,
        limit,
    ):
        out = []
        for cid, c in clusters.items():
            if not c.linked or c.embedding is None or c.kind == "market_reaction":
                continue
            if (c.first_published_at, cid) >= (first_published_at, cluster_id):
                continue
            if c.first_published_at < window_start:
                continue
            score = _cosine(c.embedding, embedding)
            shares = any(m.company_ids & company_ids for m in c.members)
            if score >= no_share_min or (shares and score >= shared_min):
                out.append(CandidateRow(cid, c.first_published_at, score, c.kind))
        out.sort(key=lambda r: (r.score, r.first_published_at, r.cluster_id), reverse=True)
        return out[:limit]

    def save_issue_kind(cluster_id, kind, reason, model, prompt_version):
        saved["kinds"].append((cluster_id, kind, model, prompt_version))
        if clusters[cluster_id].kind is not None:
            return False
        clusters[cluster_id].kind, clusters[cluster_id].kind_reason = kind, reason
        return True

    def save_link_votes(run_id, cluster_id, rows):
        saved["votes"].append((run_id, cluster_id, rows))
        return len(rows)

    def record_link_decision(cluster_id, parent_id, score, relation, *, relink=False):
        c = clusters[cluster_id]
        if c.linked != relink:
            return False
        c.linked, c.parent, c.score, c.relation = True, parent_id, score, relation
        c.root = (clusters[parent_id].root or parent_id) if parent_id else cluster_id
        return True

    def embed_texts(texts, dim, model=None):
        return [[1.0, float(len(t) % 5)] for t in texts]

    def fetch_relink_tail(after_published_at, after_id, relink_since):
        rows = sorted(
            (
                (cid, c)
                for cid, c in clusters.items()
                if c.linked
                and c.embedding is not None
                and (c.first_published_at, cid) > (after_published_at, after_id)
                and c.first_published_at >= relink_since
            ),
            key=lambda pair: (pair[1].first_published_at, pair[0]),
        )
        return [
            LinkTarget(
                cid, c.title, c.first_published_at, embedding=c.embedding, parent_id=c.parent
            )
            for cid, c in rows
        ]

    def release_link_decisions(cluster_ids):
        saved["released"].extend(cluster_ids)
        for cid in cluster_ids:
            clusters[cid].linked = False
        return len(cluster_ids)

    for name, fn in {
        "fetch_link_targets": fetch_link_targets,
        "fetch_cluster_members": fetch_cluster_members,
        "fetch_relink_tail": fetch_relink_tail,
        "release_link_decisions": release_link_decisions,
        "embed_texts": embed_texts,
        "save_cluster_embedding": lambda cid, emb: None,
        "record_link_decision": record_link_decision,
        "get_company_matcher": lambda: MATCHER,
        "fetch_issue_rows": fetch_issue_rows,
        "fetch_company_names": fetch_company_names,
        "fetch_vote_candidates": fetch_vote_candidates,
        "save_issue_kind": save_issue_kind,
        "save_link_votes": save_link_votes,
        "PostgresCallStore": lambda: store,
        "_bedrock_invoker": lambda workers: bedrock,
    }.items():
        monkeypatch.setattr(job, name, fn)

    yield job, clusters, bedrock, saved, store
    config.get_news_settings.cache_clear()
    common_config.get_settings.cache_clear()


def _story(clusters, now: datetime) -> None:
    for cid, title, days in ((1, PROPOSAL, 10), (2, REACTION, 9), (3, FAILURE, 2)):
        clusters[cid] = FakeCluster(
            title,
            now - timedelta(days=days),
            members=[ClusterMember(title, frozenset({GABIA}), now - timedelta(days=days))],
        )


def test_market_reaction_is_root_and_never_a_candidate(vote):
    job, clusters, bedrock, saved, _ = vote
    _story(clusters, now_kst())
    bedrock.kinds[REACTION] = "market_reaction"
    # 투표자는 주가 반응 이슈와의 쌍도 받아들이겠지만, 후보에 오르지 않아야 한다.
    bedrock.pairs = {
        (PROPOSAL, FAILURE): PairScript(rank_key="공개매수"),
        (REACTION, FAILURE): PairScript(rank_key="가비아"),
    }

    stats = job.run()

    assert (clusters[3].parent, clusters[3].root, clusters[3].relation) == (1, 1, "follow_up")
    assert (clusters[2].parent, clusters[2].root, clusters[2].kind) == (None, 2, "market_reaction")
    assert clusters[1].kind == clusters[3].kind == "event"
    assert stats["scanned"] == 3
    assert (stats["linked"], stats["follow_up"], stats["roots"]) == (1, 1, 2)
    # 1차 모델이 주가 반응으로 본 2 만 성격 분류 모델이 다시 판정한다.
    assert (stats["classified"], stats["market_reaction"], stats["kind_escalated"]) == (3, 1, 1)
    assert bedrock.kind_models == {KIMI: 3, SONNET: 1}
    assert (stats["proposed"], stats["confirmed"], stats["failed"], stats["deferred"]) == (
        1,
        1,
        0,
        0,
    )
    assert stats["llm_calls"] == sum(bedrock.calls.values()) > 0
    assert stats["llm_cost_usd"] > 0
    # 분류는 최종 답을 낸 모델·프롬프트 버전과 함께 남는다.
    assert (1, "event", KIMI, "issue-kind-v4") in saved["kinds"]
    assert (2, "market_reaction", SONNET, "issue-kind-v4") in saved["kinds"]
    # 3 의 표: 후보는 1 하나(2 는 주가 반응이라 빠진다), 그것이 고른 부모다.
    run_id, cluster_id, rows = next(v for v in saved["votes"] if v[1] == 3)
    assert [r.candidate_cluster_id for r in rows] == [1]
    row = rows[0]
    assert (row.pair_vote, row.rank_vote, row.judge_vote, row.check_vote) == (
        True,
        True,
        True,
        True,
    )
    assert (row.vote_count, row.accepted, row.chosen, row.judge_by) == (
        4,
        True,
        True,
        "scope_judge",
    )
    assert row.check_role == "NEW_STEP"
    assert clusters[3].score == pytest.approx(max(row.event_score, row.cosine))
    # 실행 한 번에 남긴 표는 같은 run_id 를 쓴다.
    assert {v[0] for v in saved["votes"]} == {run_id}


def test_kind_model_overrules_the_screen_model(vote):
    job, clusters, bedrock, saved, _ = vote
    _story(clusters, now_kst())
    # 1차 모델이 주가 반응으로 본 1 을 성격 분류 모델이 event 로 되돌린다.
    bedrock.model_kinds[(KIMI, PROPOSAL)] = "market_reaction"
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}

    stats = job.run()

    assert (1, "event", SONNET, "issue-kind-v4") in saved["kinds"]
    assert (stats["market_reaction"], stats["kind_escalated"]) == (0, 1)
    assert clusters[3].parent == 1


def test_empty_screen_model_classifies_with_the_kind_model_alone(vote, monkeypatch):
    from pipelines.common import config as common_config

    job, clusters, bedrock, saved, _ = vote
    monkeypatch.setenv("BEDROCK_ISSUE_KIND_SCREEN_MODEL", "")
    common_config.get_settings.cache_clear()
    _story(clusters, now_kst())
    bedrock.kinds[REACTION] = "market_reaction"

    stats = job.run()

    assert bedrock.kind_models == {SONNET: 3}
    assert {model for _, _, model, _ in saved["kinds"]} == {SONNET}
    assert (stats["market_reaction"], stats["kind_escalated"]) == (1, 0)


def test_llm_failure_of_one_issue_does_not_fail_the_run(vote):
    job, clusters, bedrock, _, _ = vote
    _story(clusters, now_kst())
    bedrock.fail_titles = {REACTION}
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}

    stats = job.run()

    assert stats["failed"] == 1
    assert not clusters[2].linked and clusters[2].kind is None
    assert clusters[3].parent == 1


def _kind_rows(store: MemoryStore, cluster_id: int) -> list:
    return [
        r
        for r in store.rows.values()
        if r.stage.startswith("issue_kind") and r.cluster_id == cluster_id
    ]


def test_unparsed_target_kind_is_not_saved_and_the_target_is_retried(vote):
    job, clusters, bedrock, saved, store = vote
    _story(clusters, now_kst())
    bedrock.garbled_kinds = {FAILURE}
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}

    stats = job.run()

    # 읽지 못한 성격은 저장하지 않고, 대상은 실패로 세어 판정 전으로 남는다.
    assert (stats["failed"], stats["kind_unknown"], stats["classified"]) == (1, 1, 2)
    assert not clusters[3].linked and clusters[3].kind is None
    assert all(cid != 3 for cid, *_ in saved["kinds"])
    assert _kind_rows(store, 3) == []

    # 다음 실행은 캐시가 아니라 모델에 다시 묻고 이어서 잇는다.
    bedrock.garbled_kinds = set()
    stats = job.run()

    assert (stats["failed"], stats["kind_unknown"], stats["classified"]) == (0, 0, 1)
    assert (clusters[3].kind, clusters[3].parent) == ("event", 1)


def test_unparsed_candidate_kind_is_left_out_without_saving(vote):
    job, clusters, bedrock, saved, store = vote
    _story(clusters, now_kst())
    # 1 은 성격 없이 이미 판정된 이슈다(예: cosine 방식으로 이은 이슈).
    clusters[1].linked = True
    bedrock.garbled_kinds = {PROPOSAL}
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}

    stats = job.run()

    # 1 은 2·3 의 후보로 나오지만 한 번만 묻고 세며, 후보에서 빠져 3 은 루트가 된다.
    assert (stats["failed"], stats["kind_unknown"], stats["roots"]) == (0, 1, 2)
    assert clusters[1].kind is None
    assert all(cid != 1 for cid, *_ in saved["kinds"])
    assert (clusters[3].linked, clusters[3].parent) == (True, None)
    assert _kind_rows(store, 1) == []
    asked = [
        r
        for r in bedrock.requests
        if r["system"][0]["text"] == kind_prompt.SYSTEM
        and f"제목: {PROPOSAL}" in r["messages"][0]["content"][0]["text"]
    ]
    # 1차 모델과 성격 분류 모델에 형식 재요청까지 한 번씩 묻는다.
    assert len(asked) == 4
    _, _, rows = next(v for v in saved["votes"] if v[1] == 3)
    assert 1 not in [r.candidate_cluster_id for r in rows]


def test_run_cap_defers_the_rest_and_next_run_continues(vote, monkeypatch):
    job, clusters, bedrock, _, _ = vote
    from pipelines.news import config

    _story(clusters, now_kst())
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}
    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN", "1")
    config.get_news_settings.cache_clear()

    stats = job.run()

    # 1 은 분류 한 번으로 루트(부모가 없는 타임라인 첫 이슈)가 되고, 2 를 분류할 때 상한에 닿아
    # 2·3 은 다음 실행으로 미뤄진다.
    assert (stats["llm_calls"], stats["deferred"], stats["failed"]) == (1, 2, 0)
    assert clusters[1].linked and not clusters[2].linked and not clusters[3].linked

    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN", "1000")
    config.get_news_settings.cache_clear()
    stats = job.run()
    assert stats["scanned"] == 2 and stats["deferred"] == 0
    assert clusters[3].parent == 1


def test_issue_cap_defers_one_issue_and_cache_carries_progress(vote, monkeypatch):
    job, clusters, bedrock, _, _ = vote
    from pipelines.news import config

    _story(clusters, now_kst())
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}
    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE", "4")
    config.get_news_settings.cache_clear()

    first = job.run()
    assert first["deferred"] == 1
    assert not clusters[3].linked
    calls_before = sum(bedrock.calls.values())

    monkeypatch.setenv("NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE", "150")
    config.get_news_settings.cache_clear()
    second = job.run()

    assert clusters[3].parent == 1
    # 첫 실행에서 받은 응답(분류, screen, verify 일부)은 캐시에서 다시 쓴다.
    assert second["cache_hits"] >= 3
    assert second["llm_calls"] == sum(bedrock.calls.values()) - calls_before


def test_cosine_method_calls_no_llm(vote, monkeypatch):
    job, clusters, bedrock, saved, _ = vote
    from pipelines.news import config

    monkeypatch.setenv("NEWS_ISSUE_LINK_METHOD", "cosine")
    config.get_news_settings.cache_clear()
    _story(clusters, now_kst())

    def fetch_link_candidates(**kwargs):
        return []

    monkeypatch.setattr(job, "fetch_link_candidates", fetch_link_candidates)
    stats = job.run()

    assert stats["roots"] == 3
    assert sum(bedrock.calls.values()) == 0
    assert saved["kinds"] == [] and saved["votes"] == []
    assert stats["llm_calls"] == 0


@pytest.mark.parametrize("relink_whole_window", [False, True])
def test_backfill_tail_relinks_issues_judged_before_a_late_target(vote, relink_whole_window):
    """옛 대상이 실패해 미뤄지는 동안 뒤 이슈가 그 대상 없이 판정된 경우다. 스케줄 실행의 재판정
    기간 밖이면 그대로 남고, 백필처럼 재판정 대상을 고르는 범위를 기간 전체로 넓히면 다시 판정돼
    부모를 찾는다."""

    job, clusters, bedrock, _, _ = vote
    now = now_kst()
    since = now - timedelta(days=30)
    _story(clusters, now)
    bedrock.kinds[REACTION] = "market_reaction"
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}
    bedrock.fail_titles = {PROPOSAL}
    relink_since = since if relink_whole_window else None

    first = job.link_pending(since=since, limit=10, relink_since=relink_since)

    assert first["failed"] == 1 and not clusters[1].linked
    assert (clusters[3].parent, clusters[3].root) == (None, 3)

    bedrock.fail_titles = set()
    second = job.link_pending(since=since, limit=10, relink_since=relink_since)

    assert clusters[1].linked
    if relink_whole_window:
        assert second["relinked"] == 2 and second["relink_changed"] == 1
        assert (clusters[3].parent, clusters[3].root) == (1, 1)
        # 주가 반응 이슈는 다시 판정해도 루트다.
        assert (clusters[2].parent, clusters[2].root) == (None, 2)
    else:
        assert second["relinked"] == 0
        assert (clusters[3].parent, clusters[3].root) == (None, 3)


@pytest.mark.parametrize("budget", ["issue", "run"])
def test_deferred_tail_is_released_for_the_next_run(vote, monkeypatch, budget):
    """상한 때문에 다시 판정하지 못한 재판정 대상은 linked_at 만 비워 다음 실행의 대상이 된다. 다음
    실행이 더 늦게 시작한 대상을 기준으로 재판정 대상을 고르면 그 이슈를 다시 고르지 못하기
    때문이다."""

    from pipelines.news.transformers.issue_link_vote.llm import (
        IssueBudgetExceeded,
        RunBudgetExceeded,
    )

    job, clusters, bedrock, saved, _ = vote
    now = now_kst()
    since = now - timedelta(days=30)
    _story(clusters, now)
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}
    # 1 은 아직 대상이고, 2·3 은 1 없이 이미 판정됐다.
    for cid in (2, 3):
        clusters[cid].linked, clusters[cid].root, clusters[cid].kind = True, cid, "event"

    original = job._VoteRun.link

    def link(self, target, embedding, relink):
        if relink and target.cluster_id == 2:
            raise (IssueBudgetExceeded if budget == "issue" else RunBudgetExceeded)("상한")
        return original(self, target, embedding, relink)

    monkeypatch.setattr(job._VoteRun, "link", link)
    stats = job.link_pending(since=since, limit=10, relink_since=since)

    assert clusters[1].linked
    if budget == "issue":
        # 2 만 미루고 3 은 다시 판정해 1 을 부모로 찾는다.
        assert saved["released"] == [2]
        assert stats["deferred"] == 1 and stats["relinked"] == 1
        assert clusters[3].parent == 1
    else:
        # 실행당 상한이면 2 와 그 뒤 3 을 모두 미룬다.
        assert saved["released"] == [2, 3]
        assert stats["deferred"] == 2 and stats["relinked"] == 0
    assert not clusters[2].linked

    monkeypatch.setattr(job._VoteRun, "link", original)
    job.link_pending(since=since, limit=10, relink_since=since)

    assert clusters[2].linked and clusters[3].linked
    assert (clusters[3].parent, clusters[3].root) == (1, 1)


def test_llm_call_cap_lowers_the_run_cap(vote, monkeypatch):
    job, clusters, bedrock, _, _ = vote
    _story(clusters, now_kst())
    bedrock.pairs = {(PROPOSAL, FAILURE): PairScript(rank_key="공개매수")}

    stats = job.link_pending(since=now_kst() - timedelta(days=30), limit=10, llm_call_cap=1)

    # 실행당 상한(1500)보다 작은 상한을 적용한다. 1 을 분류한 뒤 2 에서 멈춘다.
    assert (stats["llm_calls"], stats["deferred"]) == (1, 2)
    assert clusters[1].linked and not clusters[2].linked
