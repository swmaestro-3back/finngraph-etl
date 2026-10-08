"""이슈 연결 투표 판정에 쓰는 저장소다. 이슈 상세, 기업 이름 정보, 후보 범위를 읽고 이슈 성격, 투표,
LLM 응답 캐시를 쓴다.

판정 규칙은 transformers/issue_link_vote, 흐름은 jobs/link_issues.py 에 있다. 연결 결과
(부모·관계·점수)는 news_clusters.record_link_decision 이 쓴다. 이 모듈의 쓰기는
news_clusters.updated_at 을 바꾸지 않는다. 승격·제목 재시도와 Event 스캔이 updated_at 범위로
대상을 고르기 때문이다.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.issue_link_vote.issue import CompanyName
from pipelines.news.transformers.issue_link_vote.llm import CallRecord

# 이슈 요약문에 쓰는 필드다. 요약과 핵심 포인트는 대표 기사에서 가져온다.
SELECT_ISSUE_ROWS_SQL = text(
    """
    SELECT c.id,
           c.title,
           c.first_published_at,
           c.last_published_at,
           c.issue_kind,
           n.summary,
           n.summary_points
      FROM news_clusters c
      LEFT JOIN news n ON n.id = c.representative_news_id
     WHERE c.id = ANY(:cluster_ids);
    """
)

# 기업 이름 정보로 companies.name, ticker, 개체 사전 별칭(사전순)을 읽는다.
SELECT_COMPANY_NAMES_SQL = text(
    """
    SELECT c.id,
           c.name,
           c.ticker,
           COALESCE(
             ARRAY_AGG(g.alias ORDER BY g.alias) FILTER (WHERE g.alias IS NOT NULL),
             '{}'
           ) AS aliases
      FROM companies c
      LEFT JOIN entity_gazetteer g ON g.company_id = c.id
     WHERE c.id = ANY(:company_ids)
     GROUP BY c.id, c.name, c.ticker;
    """
)

# 투표 판정 후보를 읽는다. 대상보다 먼저 시작했고((first_published_at, id) 순서) lookback 안이며
# 판정과 임베딩이 끝난 클러스터 중, 주가 반응으로 분류되지 않은 것이 후보다. 아직 분류하지 않은
# 클러스터도 포함하며, job 이 분류한 뒤 걸러 낸다. 코사인 하한은 :no_share_min 이고, 대상의 연결
# 기업 중 하나라도 연결된 클러스터는 :shared_min 이다. 둘 다 기업이 없는 쌍에 적용하는 더 높은
# 하한은 파이썬(universe.eligible)에서 다시 확인한다. 코사인 내림차순으로 :limit 개까지 읽는다.
SELECT_VOTE_CANDIDATES_SQL = text(
    """
    SELECT s.id, s.first_published_at, s.score, s.issue_kind
      FROM (
        SELECT c.id,
               c.first_published_at,
               c.issue_kind,
               1 - (c.embedding <=> CAST(:embedding AS vector)) AS score,
               EXISTS (
                 SELECT 1
                   FROM news n
                   JOIN news_companies x ON x.news_id = n.id
                  WHERE n.cluster_id = c.id
                    AND x.company_id = ANY(CAST(:company_ids AS BIGINT[]))
               ) AS shares_company
          FROM news_clusters c
         WHERE c.linked_at IS NOT NULL
           AND c.embedding IS NOT NULL
           AND (c.issue_kind IS NULL OR c.issue_kind = 'event')
           AND (c.first_published_at, c.id)
               < (CAST(:first_published_at AS TIMESTAMPTZ), CAST(:cluster_id AS BIGINT))
           AND c.first_published_at >= :window_start
      ) s
     WHERE s.score >= :no_share_min
        OR (s.shares_company AND s.score >= :shared_min)
     ORDER BY s.score DESC, s.first_published_at DESC, s.id DESC
     LIMIT :limit;
    """
)

# 분류는 한 번만 쓴다. 두 실행이 겹치면 먼저 쓴 분류가 남는다.
UPDATE_ISSUE_KIND_SQL = text(
    """
    UPDATE news_clusters
       SET issue_kind = :kind,
           issue_kind_reason = :reason,
           issue_kind_model = :model,
           issue_kind_prompt_version = :prompt_version,
           issue_kind_at = now()
     WHERE id = :cluster_id
       AND issue_kind IS NULL;
    """
)

SELECT_LLM_CALL_SQL = text(
    """
    SELECT response
      FROM news_issue_link_llm_calls
     WHERE cache_key = :cache_key;
    """
)

DELETE_LLM_CALL_SQL = text(
    """
    DELETE FROM news_issue_link_llm_calls
     WHERE cache_key = :cache_key;
    """
)

INSERT_LLM_CALL_SQL = text(
    """
    INSERT INTO news_issue_link_llm_calls (
        cache_key, stage, model, prompt_version, sample, cluster_id, candidate_cluster_id,
        response, input_tokens, output_tokens, latency_ms
    )
    VALUES (
        :cache_key, :stage, :model, :prompt_version, :sample, :cluster_id, :candidate_cluster_id,
        CAST(:response AS jsonb), :input_tokens, :output_tokens, :latency_ms
    )
    ON CONFLICT (cache_key) DO NOTHING
    RETURNING response;
    """
)

INSERT_LINK_VOTE_SQL = text(
    """
    INSERT INTO news_issue_link_votes (
        run_id, cluster_id, candidate_cluster_id, cosine, event_score, gap_hours,
        pair_vote, rank_vote, judge_vote, check_vote, pair_route, judge_by,
        pair_label, rank_label, judge_label, judge_a_scope, judge_b_scope, check_role,
        vote_count, accepted, chosen, evidence
    )
    VALUES (
        CAST(:run_id AS UUID), :cluster_id, :candidate_cluster_id, :cosine, :event_score,
        :gap_hours, :pair_vote, :rank_vote, :judge_vote, :check_vote, :pair_route, :judge_by,
        :pair_label, :rank_label, :judge_label, :judge_a_scope, :judge_b_scope, :check_role,
        :vote_count, :accepted, :chosen, CAST(:evidence AS jsonb)
    )
    ON CONFLICT (run_id, cluster_id, candidate_cluster_id) DO NOTHING;
    """
)


@dataclass(frozen=True)
class IssueRow:
    cluster_id: int
    title: str
    first_published_at: datetime
    last_published_at: datetime
    issue_kind: str | None = None
    summary: str | None = None
    summary_points: Any = None


@dataclass(frozen=True)
class CandidateRow:
    cluster_id: int
    first_published_at: datetime
    score: float
    issue_kind: str | None = None


@dataclass(frozen=True)
class LinkVoteRow:
    """news_issue_link_votes 의 한 행이다. 묻지 않은 투표자의 표는 None 이다."""

    candidate_cluster_id: int
    cosine: float
    event_score: float | None
    gap_hours: float
    pair_vote: bool | None
    rank_vote: bool | None
    judge_vote: bool | None
    check_vote: bool | None
    pair_route: int | None
    judge_by: str | None
    pair_label: str | None
    rank_label: str | None
    judge_label: str | None
    judge_a_scope: str | None
    judge_b_scope: str | None
    check_role: str | None
    vote_count: int
    accepted: bool
    chosen: bool
    evidence: dict[str, Any]


def fetch_issue_rows(cluster_ids: list[int]) -> dict[int, IssueRow]:
    """클러스터별 제목·기간·성격과 대표 기사 요약을 읽는다. 없는 id 는 결과에 키가 없다."""

    if not cluster_ids:
        return {}
    with session_scope() as session:
        rows = session.execute(
            SELECT_ISSUE_ROWS_SQL, {"cluster_ids": sorted({int(c) for c in cluster_ids})}
        ).fetchall()
    return {
        int(row.id): IssueRow(
            cluster_id=int(row.id),
            title=row.title or "",
            first_published_at=row.first_published_at,
            last_published_at=row.last_published_at,
            issue_kind=row.issue_kind,
            summary=row.summary,
            summary_points=row.summary_points,
        )
        for row in rows
    }


def fetch_company_names(company_ids: list[int]) -> dict[int, CompanyName]:
    if not company_ids:
        return {}
    with session_scope() as session:
        rows = session.execute(
            SELECT_COMPANY_NAMES_SQL, {"company_ids": sorted({int(c) for c in company_ids})}
        ).fetchall()
    return {
        int(row.id): CompanyName(
            name=row.name or "", ticker=row.ticker, aliases=tuple(row.aliases or ())
        )
        for row in rows
    }


def fetch_vote_candidates(
    *,
    cluster_id: int,
    embedding: str,
    first_published_at: datetime,
    company_ids: frozenset[int],
    window_start: datetime,
    shared_min: float,
    no_share_min: float,
    limit: int,
) -> list[CandidateRow]:
    """대상의 투표 판정 후보를 SELECT_VOTE_CANDIDATES_SQL 조건으로 읽는다. company_ids 는 대상의
    연결 기업이다."""

    with session_scope() as session:
        rows = session.execute(
            SELECT_VOTE_CANDIDATES_SQL,
            {
                "cluster_id": cluster_id,
                "embedding": embedding,
                "first_published_at": first_published_at,
                "company_ids": sorted(company_ids),
                "window_start": window_start,
                "shared_min": shared_min,
                "no_share_min": no_share_min,
                "limit": limit,
            },
        ).fetchall()
    return [
        CandidateRow(
            cluster_id=int(row.id),
            first_published_at=row.first_published_at,
            score=float(row.score),
            issue_kind=row.issue_kind,
        )
        for row in rows
    ]


def save_issue_kind(
    cluster_id: int, kind: str, reason: str, model: str, prompt_version: str
) -> bool:
    """이슈 성격을 쓴다. 이미 분류돼 있으면 쓰지 않고 False 를 돌려준다."""

    with session_scope() as session:
        updated = session.execute(
            UPDATE_ISSUE_KIND_SQL,
            {
                "cluster_id": cluster_id,
                "kind": kind,
                "reason": reason,
                "model": model,
                "prompt_version": prompt_version,
            },
        ).rowcount
    return updated > 0


def save_link_votes(run_id: uuid.UUID, cluster_id: int, rows: list[LinkVoteRow]) -> int:
    """대상 하나의 후보별 표를 쓰고 쓴 행 수를 돌려준다. 같은 실행·쌍이 이미 있으면 건너뛴다."""

    if not rows:
        return 0
    params = [
        {
            "run_id": str(run_id),
            "cluster_id": cluster_id,
            "candidate_cluster_id": row.candidate_cluster_id,
            "cosine": row.cosine,
            "event_score": row.event_score,
            "gap_hours": row.gap_hours,
            "pair_vote": row.pair_vote,
            "rank_vote": row.rank_vote,
            "judge_vote": row.judge_vote,
            "check_vote": row.check_vote,
            "pair_route": row.pair_route,
            "judge_by": row.judge_by,
            "pair_label": row.pair_label,
            "rank_label": row.rank_label,
            "judge_label": row.judge_label,
            "judge_a_scope": row.judge_a_scope,
            "judge_b_scope": row.judge_b_scope,
            "check_role": row.check_role,
            "vote_count": row.vote_count,
            "accepted": row.accepted,
            "chosen": row.chosen,
            "evidence": json.dumps(row.evidence, ensure_ascii=False),
        }
        for row in rows
    ]
    with session_scope() as session:
        result = session.execute(INSERT_LINK_VOTE_SQL, params)
    return max(int(result.rowcount or 0), 0)


class PostgresCallStore:
    """news_issue_link_llm_calls 에 두는 LLM 응답 캐시다. issue_link_vote.llm.CallStore 를
    구현한다."""

    def get(self, cache_key: str) -> dict[str, Any] | None:
        with session_scope() as session:
            row = session.execute(SELECT_LLM_CALL_SQL, {"cache_key": cache_key}).first()
        return None if row is None else row.response

    def put(self, record: CallRecord) -> dict[str, Any]:
        """같은 키에는 먼저 쓴 응답만 남는다. 다른 실행이 먼저 썼으면 그 응답을 돌려준다."""

        with session_scope() as session:
            row = session.execute(
                INSERT_LLM_CALL_SQL,
                {
                    "cache_key": record.cache_key,
                    "stage": record.stage,
                    "model": record.model,
                    "prompt_version": record.prompt_version,
                    "sample": record.sample,
                    "cluster_id": record.cluster_id,
                    "candidate_cluster_id": record.candidate_cluster_id,
                    "response": json.dumps(record.response, ensure_ascii=False),
                    "input_tokens": record.input_tokens,
                    "output_tokens": record.output_tokens,
                    "latency_ms": record.latency_ms,
                },
            ).first()
            if row is not None:
                return row.response
            existing = session.execute(SELECT_LLM_CALL_SQL, {"cache_key": record.cache_key}).first()
        return existing.response if existing is not None else record.response

    def delete(self, cache_key: str) -> None:
        with session_scope() as session:
            session.execute(DELETE_LLM_CALL_SQL, {"cache_key": cache_key})
