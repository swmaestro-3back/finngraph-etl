"""투표 판정 저장소 통합 테스트다. 이슈 상세·기업 이름 정보·후보 범위 조회와 이슈 성격·투표·LLM
응답 캐시 쓰기·삭제를 확인한다.

실제 Postgres(pgvector)에 news / news_clusters / news_companies / companies / entity_gazetteer 행을
만들고 SQL 을 직접 부른다. 다른 행과 섞이지 않게 2102년 날짜와 uuid 마커를 쓴다. V13 마이그레이션이
적용된 DB 가 필요하다.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from pipelines.common.clients.postgres import session_scope
from pipelines.news.repositories.postgres.issue_links import (
    LinkVoteRow,
    PostgresCallStore,
    fetch_company_names,
    fetch_issue_rows,
    fetch_vote_candidates,
    save_issue_kind,
    save_link_votes,
)
from pipelines.news.transformers.issue_link_vote.issue import CompanyName
from pipelines.news.transformers.issue_link_vote.llm import CallRecord
from pipelines.news.transformers.issue_linker import to_vector_literal
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

pytestmark = pytest.mark.integration

BASE = datetime(2102, 5, 1, 9, tzinfo=SEOUL_TIMEZONE)


def _vector(x: float, y: float) -> str:
    return to_vector_literal([x, y] + [0.0] * 1022)


@pytest.fixture
def story():
    """이슈 7개를 만든다. target(5/20, 기업 A), proposal(5/10, A, 코사인 0.6), reaction(5/11, A,
    주가 반응), other(5/12, 기업 B, 코사인 0.5), far(5/13, B, 코사인 0.2), unjudged(5/14, A, 판정
    전), later(5/21, A)."""

    marker = uuid.uuid4().hex[:8]
    with session_scope() as session:
        company_a, company_b = (
            session.execute(
                text(
                    "INSERT INTO companies (name, ticker, is_listed, country) "
                    "VALUES (:n, :t, true, 'KR') RETURNING id"
                ),
                {"n": f"콤보{marker}{suffix}", "t": None},
            ).scalar_one()
            for suffix in ("A", "B")
        )
        stock_id = session.execute(
            text(
                "INSERT INTO stocks (company_id, ticker, name, market) "
                "VALUES (:c, :t, :n, 'KOSPI') RETURNING id"
            ),
            {"c": company_a, "t": f"T{marker[:5]}", "n": f"콤보{marker}A"},
        ).scalar_one()
        for alias in (f"콤보{marker}A", f"콤보약칭{marker}"):
            session.execute(
                text(
                    "INSERT INTO entity_gazetteer "
                    "(alias, company_id, stock_id, ticker, canonical_name, alias_source) "
                    "VALUES (:a, :c, :s, :t, :n, 'CURATED')"
                ),
                {
                    "a": alias,
                    "c": company_a,
                    "s": stock_id,
                    "t": f"T{marker[:5]}",
                    "n": f"콤보{marker}A",
                },
            )

        def cluster(title: str, day: int, vector: str, *, linked: bool, kind: str | None = None):
            cid = session.execute(
                text(
                    """
                    INSERT INTO news_clusters (
                        first_published_at, last_published_at, original_size, member_count,
                        title, embedding, linked_at, issue_kind
                    )
                    VALUES (:t, :t, 3, 3, :title, CAST(:v AS vector), :linked, :kind)
                    RETURNING id
                    """
                ),
                {
                    "t": BASE + timedelta(days=day),
                    "title": f"{title} {marker}",
                    "v": vector,
                    "linked": BASE if linked else None,
                    "kind": kind,
                },
            ).scalar_one()
            return cid

        def member(cluster_id: int, company_id: int, *, representative: bool = False) -> None:
            news_id = session.execute(
                text(
                    """
                    INSERT INTO news (title, text, link, published_at, cluster_id, summary)
                    VALUES ('기사', '본문', :link, :p, :c, :s)
                    RETURNING id
                    """
                ),
                {
                    "link": f"https://example.com/vote/{marker}/{cluster_id}",
                    "p": BASE,
                    "c": cluster_id,
                    "s": "요약 문단이에요.",
                },
            ).scalar_one()
            session.execute(
                text("INSERT INTO news_companies (news_id, company_id) VALUES (:n, :c)"),
                {"n": news_id, "c": company_id},
            )
            if representative:
                session.execute(
                    text("UPDATE news_clusters SET representative_news_id = :n WHERE id = :c"),
                    {"n": news_id, "c": cluster_id},
                )

        ids = {
            "proposal": cluster("공개매수 추진", 9, _vector(0.6, 0.8), linked=True, kind="event"),
            "reaction": cluster(
                "주가 급등", 10, _vector(1.0, 0.0), linked=True, kind="market_reaction"
            ),
            "other": cluster("다른 기업", 11, _vector(0.5, 0.866), linked=True),
            "far": cluster("먼 기업", 12, _vector(0.2, 0.98), linked=True),
            "unjudged": cluster("판정 전", 13, _vector(1.0, 0.0), linked=False),
            "target": cluster("공개매수 실패", 19, _vector(1.0, 0.0), linked=False),
            "later": cluster("나중", 20, _vector(1.0, 0.0), linked=True),
        }
        for key in ("proposal", "reaction", "unjudged", "target", "later"):
            member(ids[key], company_a, representative=key == "target")
        for key in ("other", "far"):
            member(ids[key], company_b)

    yield {**ids, "a": company_a, "b": company_b, "marker": marker}

    with session_scope() as session:
        session.execute(
            text("DELETE FROM news_clusters WHERE id = ANY(:ids)"), {"ids": list(ids.values())}
        )
        session.execute(
            text("DELETE FROM news WHERE link LIKE :p"),
            {"p": f"https://example.com/vote/{marker}/%"},
        )
        session.execute(text("DELETE FROM stocks WHERE id = :s"), {"s": stock_id})
        session.execute(
            text("DELETE FROM companies WHERE id = ANY(:ids)"), {"ids": [company_a, company_b]}
        )


def test_issue_rows_and_company_names(story):
    rows = fetch_issue_rows([story["target"], story["reaction"], -1])

    assert set(rows) == {story["target"], story["reaction"]}
    target = rows[story["target"]]
    assert target.title == f"공개매수 실패 {story['marker']}"
    assert target.summary == "요약 문단이에요."
    assert target.issue_kind is None
    assert rows[story["reaction"]].issue_kind == "market_reaction"
    # 대표 기사가 없으면 요약이 없다.
    assert rows[story["reaction"]].summary is None
    assert fetch_issue_rows([]) == {}

    names = fetch_company_names([story["a"], story["b"]])
    marker = story["marker"]
    a = names[story["a"]]
    assert (a.name, a.ticker) == (f"콤보{marker}A", None)
    # 별칭 순서는 DB 정렬 규칙을 따르므로 비교하지 않는다. 쓰는 곳도 순서에 기대지 않는다.
    assert sorted(a.aliases) == sorted((f"콤보{marker}A", f"콤보약칭{marker}"))
    assert names[story["b"]] == CompanyName(f"콤보{marker}B", None, ())


def test_vote_candidates_rules(story):
    rows = fetch_vote_candidates(
        cluster_id=story["target"],
        embedding=_vector(1.0, 0.0),
        first_published_at=BASE + timedelta(days=19),
        company_ids=frozenset({story["a"]}),
        window_start=BASE,
        shared_min=0.10,
        no_share_min=0.45,
        limit=10,
    )

    # 판정 전(unjudged)·늦게 시작(later)·주가 반응(reaction)은 빠지고, 기업이 겹치지 않는 쌍은 0.45
    # 이상(other)만 든다. 결과는 코사인 내림차순이다.
    assert [r.cluster_id for r in rows] == [story["proposal"], story["other"]]
    assert rows[0].score == pytest.approx(0.6, abs=1e-6)
    assert rows[1].issue_kind is None

    # 대상에 기업이 없으면 no_share 하한만 남는다.
    rows = fetch_vote_candidates(
        cluster_id=story["target"],
        embedding=_vector(1.0, 0.0),
        first_published_at=BASE + timedelta(days=19),
        company_ids=frozenset(),
        window_start=BASE,
        shared_min=0.10,
        no_share_min=0.55,
        limit=10,
    )
    assert [r.cluster_id for r in rows] == [story["proposal"]]


def test_issue_kind_is_written_once(story):
    assert save_issue_kind(story["target"], "event", "자기 사건", "m", "issue-kind-v1")
    assert not save_issue_kind(story["target"], "market_reaction", "다시", "m", "issue-kind-v1")

    with session_scope() as session:
        row = session.execute(
            text(
                "SELECT issue_kind, issue_kind_reason, issue_kind_model, issue_kind_prompt_version, "
                "issue_kind_at IS NOT NULL AS stamped FROM news_clusters WHERE id = :c"
            ),
            {"c": story["target"]},
        ).one()
    assert tuple(row) == ("event", "자기 사건", "m", "issue-kind-v1", True)

    with pytest.raises(IntegrityError), session_scope() as session:
        session.execute(
            text("UPDATE news_clusters SET issue_kind = 'theme' WHERE id = :c"),
            {"c": story["target"]},
        )


def _record(key: str, cluster_id: int, candidate: int | None, response: dict) -> CallRecord:
    return CallRecord(
        cache_key=key,
        stage="screen",
        model="m",
        prompt_version="pair-screen-v1",
        sample=0,
        cluster_id=cluster_id,
        candidate_cluster_id=candidate,
        response=response,
        input_tokens=10,
        output_tokens=2,
        latency_ms=120,
    )


def test_llm_call_cache_first_writer_wins(story):
    store = PostgresCallStore()
    key = f"test-{uuid.uuid4().hex}"
    assert store.get(key) is None

    first = {"text": '{"label": "SAME_STORY"}', "tool_input": None, "stop_reason": "end_turn"}
    assert store.put(_record(key, story["target"], story["proposal"], first)) == first
    second = {"text": '{"label": "DIFFERENT"}', "tool_input": None, "stop_reason": "end_turn"}
    assert store.put(_record(key, story["target"], None, second)) == first
    assert store.get(key) == first

    with session_scope() as session:
        row = session.execute(
            text(
                "SELECT stage, model, prompt_version, sample, cluster_id, candidate_cluster_id, "
                "input_tokens, output_tokens, latency_ms FROM news_issue_link_llm_calls "
                "WHERE cache_key = :k"
            ),
            {"k": key},
        ).one()
    assert tuple(row) == (
        "screen",
        "m",
        "pair-screen-v1",
        0,
        story["target"],
        story["proposal"],
        10,
        2,
        120,
    )


def test_llm_call_cache_delete(story):
    store = PostgresCallStore()
    key = f"test-{uuid.uuid4().hex}"
    broken = {"text": "생각 중", "tool_input": None, "stop_reason": "end_turn"}
    store.put(_record(key, story["target"], None, broken))

    store.delete(key)
    assert store.get(key) is None
    # 없는 키를 지워도 오류가 아니다.
    store.delete(key)

    # 지운 뒤에는 새 응답을 쓸 수 있다.
    fixed = {"text": '{"label": "DIFFERENT"}', "tool_input": None, "stop_reason": "end_turn"}
    assert store.put(_record(key, story["target"], None, fixed)) == fixed
    assert store.get(key) == fixed


def _vote(candidate: int, *, chosen: bool) -> LinkVoteRow:
    return LinkVoteRow(
        candidate_cluster_id=candidate,
        cosine=0.6,
        event_score=0.7 if chosen else None,
        gap_hours=240.0,
        pair_vote=True,
        rank_vote=chosen,
        judge_vote=True if chosen else None,
        check_vote=True if chosen else None,
        pair_route=1,
        judge_by="scope_judge" if chosen else None,
        pair_label="SAME_STORY",
        rank_label="SAME_STORY" if chosen else "DIFFERENT",
        judge_label="SAME_STORY" if chosen else None,
        judge_a_scope="SPECIFIC" if chosen else None,
        judge_b_scope="SPECIFIC" if chosen else None,
        check_role="NEW_STEP" if chosen else None,
        vote_count=4 if chosen else 1,
        accepted=chosen,
        chosen=chosen,
        evidence={"pair": {"screen": "SAME_STORY"}},
    )


def test_link_votes(story):
    run_id = uuid.uuid4()
    rows = [_vote(story["proposal"], chosen=True), _vote(story["other"], chosen=False)]
    save_link_votes(run_id, story["target"], rows)
    # 같은 실행·쌍은 다시 쓰지 않는다.
    save_link_votes(run_id, story["target"], rows[:1])
    assert save_link_votes(run_id, story["target"], []) == 0

    with session_scope() as session:
        found = session.execute(
            text(
                "SELECT candidate_cluster_id, judge_vote, vote_count, accepted, chosen, evidence "
                "FROM news_issue_link_votes WHERE run_id = :r ORDER BY vote_count DESC"
            ),
            {"r": str(run_id)},
        ).all()
    assert [tuple(r) for r in found] == [
        (story["proposal"], True, 4, True, True, {"pair": {"screen": "SAME_STORY"}}),
        (story["other"], None, 1, False, False, {"pair": {"screen": "SAME_STORY"}}),
    ]

    bad = LinkVoteRow(**{**_vote(story["proposal"], chosen=True).__dict__, "judge_by": "x"})
    with pytest.raises(IntegrityError):
        save_link_votes(uuid.uuid4(), story["target"], [bad])
