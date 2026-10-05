"""이슈 타임라인 연결 — news_cluster_articles·news_backfill_cluster_articles DAG 의
link_issues task.

이슈(이름과 대표 기사가 있는 클러스터)를 같은 이야기의 앞선 이슈에 잇는다. LLM 은 부르지 않는다 —
노드 제목은 news_clusters.title, 한 줄 요약은 summarize_articles 가 대표 기사에 남긴 핵심 포인트를
그대로 쓴다.

1. linked_at 이 NULL 인 이슈를 오래된 순으로 고른다(지금부터 NEWS_ISSUE_LINK_LOOKBACK_DAYS 안, 런당
   NEWS_ISSUE_LINK_MAX_PER_RUN 개). 대표 기사 요약이 아직 없으면 마지막 갱신 뒤 SUMMARY_WAIT 이
   지난 이슈만 요약 없이 고른다.
2. 임베딩이 없는 대상은 제목·한 줄 요약·후보 기사 제목을 한 번에 임베딩해 저장한다.
3. 꼬리를 고른다 — 대상 중 가장 먼저 시작한 것보다 뒤에 시작해 이미 판정된 이슈(지금부터
   NEWS_ISSUE_RELINK_WINDOW_HOURS 안에 시작한 것만). 대상은 시작 순서가 아니라 이름·대표·요약을
   받는 순서로 들어오므로, 먼저 시작한 이슈가 늦게 들어오면 꼬리는 그 이슈를 부모로 못 본 채
   판정돼 있다(같은 사건이 둘로 갈린 클러스터가 서로 루트로 남는다). 꼬리를 다시 판정하면 창
   안에서는 백필처럼 시작 순서로 한 번에 이은 결과와 같아진다.
4. 대상과 꼬리를 오래된 순으로 하나씩 잇는다. 판정마다 커밋하므로 같은 런의 뒤 이슈는 앞선
   이슈를 부모 후보로 본다.

클러스터 하나의 실패는 세고 넘어간다 — 대상은 linked_at 이 NULL 로 남아 다음 런에 다시 보고, 꼬리는
지난 판정이 남는다. 전부 실패하면 Bedrock·DB 장애로 보고 올려 Airflow 가 재시도하게 한다.

스케줄 진입점 run() 은 NEWS_ISSUE_LINK_ENABLED 가 켜져 있을 때만 돈다. 꼬리 창 밖의 판정은 다시
만들지 않으므로, 연결 백필 전에 돌면 lookback 밖의 옛 이슈를 부모로 못 본 채 루트로 굳는다.
같은 이유로 옛 기사를 한꺼번에 들이는 백필(news_backfill_krx100) 뒤에는 연결을 그 기간만큼 다시
만든다(pipelines/news/README.md "이슈 타임라인"). 백필 스크립트
(scripts/backfill_issue_timeline.py)는 link_pending() 을 직접 부른다.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from pipelines.common.clients.bedrock import embed_texts
from pipelines.common.gazetteer import get_company_matcher
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.news.config import NewsSettings, get_news_settings
from pipelines.news.repositories.postgres.news_clusters import (
    LinkTarget,
    fetch_cluster_members,
    fetch_link_candidates,
    fetch_link_targets,
    fetch_relink_tail,
    record_link_decision,
    save_cluster_embedding,
)
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

# news_clusters.embedding vector(1024) 스키마와 묶여 있어 설정이 아니라 코드 상수로 둔다.
ISSUE_EMBEDDING_DIM = 1024
# 부모 후보를 한 번에 읽는 수. SQL 은 "대상의 주요 기업이 연결된" 후보까지만 거르고 후보 쪽 주요
# 기업 규칙은 파이썬에서 다시 거른다. 본문에만 그 기업이 스친 후보가 위를 다 채우면 맞는 부모가
# 아래에 있어도 잘리므로, 맞는 후보가 나오거나 후보가 바닥날 때까지 다음 페이지를 읽는다.
CANDIDATE_PAGE_SIZE = 30
# 대표 기사 요약이 없는 이슈를 요약 없이 잇기까지 기다리는 시간. 클러스터 updated_at(승격·제목
# 생성 때 갱신) 기준이다 — 그동안 클러스터 DAG 런마다 요약이 다시 시도된다.
SUMMARY_WAIT = timedelta(hours=24)

# relinked 는 다시 판정한 꼬리 수, relink_changed 는 그중 부모가 바뀐 수다. linked·roots 는 이번
# 대상만 센다.
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
)


def _embed_missing(
    targets: list[LinkTarget], members: dict[int, list[ClusterMember]], model: str
) -> tuple[dict[int, str], set[int]]:
    """임베딩이 없는 대상을 한 번에 임베딩해 저장한다. (저장된 id → 벡터 텍스트, 실패 id)"""

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
    """클러스터별 주요 기업을 런 동안 기억한다. 같은 부모 후보가 여러 대상에 반복해 나온다."""

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

    SQL 정렬이 choose_parent 와 같아 페이지에서 처음 맞는 후보가 전체에서 가장 좋은 부모다.
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
    """부모 선택 → 기록. 쓴 행이 없으면(다른 런이 먼저 판정했거나 그사이 초기화됐으면) None."""

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
    targets: list[LinkTarget], now: datetime, settings: NewsSettings
) -> list[LinkTarget]:
    """이번 대상(오래된 순) 중 가장 먼저 시작한 것보다 뒤에 시작한 판정된 이슈, 창 안만."""

    if not targets or settings.issue_relink_window_hours <= 0:
        return []
    first = targets[0]
    return fetch_relink_tail(
        first.first_published_at,
        first.cluster_id,
        now - timedelta(hours=settings.issue_relink_window_hours),
    )


def run() -> dict[str, int]:
    """스케줄 진입점. 지금부터 lookback 안의 연결 대상을 런당 상한만큼 잇는다.

    NEWS_ISSUE_LINK_ENABLED 가 꺼져 있으면 아무것도 읽지 않고 0 통계를 돌려준다.
    """

    settings = get_news_settings()
    if not settings.issue_link_enabled:
        logger.info("[link_issues] NEWS_ISSUE_LINK_ENABLED=false — 건너뜀")
        return dict.fromkeys(STAT_KEYS, 0)

    return link_pending(
        since=now_kst() - timedelta(days=settings.issue_link_lookback_days),
        limit=settings.issue_link_max_per_run,
    )


def link_pending(since: datetime, limit: int) -> dict[str, int]:
    """first_published_at 이 since 이후인 연결 대상을 limit 개까지 임베딩하고, 꼬리와 함께 오래된
    순으로 잇는다.

    스위치와 무관하게 돈다. 백필 스크립트가 기간을 넓혀 부른다. linked 는 same_event + follow_up
    이다. 다른 런이 먼저 판정한 대상은 어느 칸에도 세지 않는다.
    """

    settings = get_news_settings()
    stats = dict.fromkeys(STAT_KEYS, 0)
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

    # 꼬리는 이번에 실제로 이을 대상(임베딩이 있는 것) 기준이다. 임베딩에 실패한 대상은 다음 런에
    # 들어올 때 다시 꼬리를 고른다.
    linkable = [target for target in targets if target.cluster_id in embeddings]
    tail = _relink_tail(linkable, now, settings)
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
    for target, relink in queue:
        try:
            decision = _link_one(
                target, embeddings[target.cluster_id], primary, settings, relink=relink
            )
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

    stats["failed"] = len(failed)
    logger.info(
        "[link_issues] 대상 %d → 임베딩 %d, 연결 %d(같은 사건 %d / 후속 %d), 루트 %d, "
        "꼬리 재판정 %d(부모 바뀜 %d), 실패 %d",
        stats["scanned"],
        stats["embedded"],
        stats["linked"],
        stats["same_event"],
        stats["follow_up"],
        stats["roots"],
        stats["relinked"],
        stats["relink_changed"],
        stats["failed"],
    )

    if stats["failed"] == stats["scanned"] + len(tail):
        raise RuntimeError(
            f"이슈 연결 전건 실패 ({stats['failed']}건) — Bedrock·DB 장애로 보고 재시도한다"
        )
    return stats
