"""이슈 타임라인 연결 — 이름이 붙은 클러스터를 같은 이야기의 앞선 클러스터에 잇는다.

1. 이름이 있고 linked_at 이 NULL 인 클러스터를 오래된 순으로 고른다(지금부터
   NEWS_ISSUE_LINK_LOOKBACK_DAYS 안, 런당 NEWS_ISSUE_LINK_MAX_PER_RUN 개).
2. 임베딩이 없는 대상은 제목·요약·최신 기사 제목을 한 번에 임베딩해 저장한다.
3. 대상을 오래된 순으로 하나씩 잇는다. 판정마다 커밋하므로 같은 런의 뒤 대상은 앞선 대상을
   부모 후보로 본다.

클러스터 하나의 실패는 세고 넘어간다 — linked_at 이 NULL 로 남아 다음 런에 다시 본다. 전부
실패하면 Bedrock·DB 장애로 보고 올려 Airflow 가 재시도하게 한다.

스케줄 진입점 run() 은 NEWS_ISSUE_LINK_ENABLED 가 켜져 있을 때만 돈다. 한 번 저장한 임베딩과
판정은 다시 만들지 않으므로, 요약 백필 전에 돌면 요약 없는 임베딩이 남고, 연결 백필 전에 돌면
lookback 밖의 옛 클러스터를 부모로 못 봐 루트로 굳는다. 백필 스크립트는 link_pending() 을 직접
부른다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.common.clients.bedrock import embed_texts
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.news.config import NewsSettings, get_news_settings
from pipelines.news.repositories.issue_timeline import (
    LinkTarget,
    fetch_cluster_company_ids,
    fetch_link_candidates,
    fetch_link_targets,
    fetch_recent_member_titles,
    record_link_decision,
    save_cluster_embedding,
)
from pipelines.news.transformers.issue_linker import (
    EMBEDDING_ARTICLE_TITLES,
    LinkDecision,
    build_embedding_text,
    choose_parent,
    to_vector_literal,
)

logger = get_logger(__name__)

# news_clusters.embedding vector(1024) 스키마와 묶여 있어 설정이 아니라 코드 상수로 둔다.
ISSUE_EMBEDDING_DIM = 1024
# 부모는 하나만 고르므로 후보는 상위 몇 개면 충분하다.
CANDIDATE_LIMIT = 20

STAT_KEYS = ("scanned", "embedded", "linked", "roots", "failed")


def _embed_missing(targets: list[LinkTarget], model: str) -> tuple[dict[int, str], set[int]]:
    """임베딩이 없는 대상을 한 번에 임베딩해 저장한다. (저장된 id → 벡터 텍스트, 실패 id)."""

    member_titles = fetch_recent_member_titles(
        [target.cluster_id for target in targets], per_cluster=EMBEDDING_ARTICLE_TITLES
    )
    texts = [
        build_embedding_text(target.title, target.summary, member_titles.get(target.cluster_id, []))
        for target in targets
    ]

    try:
        vectors = embed_texts(texts, dim=ISSUE_EMBEDDING_DIM, model=model)
    except Exception as e:
        logger.warning(
            "클러스터 임베딩 실패(다음 런에 재시도): %d건, %s: %s",
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
                "클러스터 임베딩 저장 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                target.cluster_id,
                type(e).__name__,
                e,
            )
            continue
        saved[target.cluster_id] = literal

    return saved, failed


def _link_one(
    target: LinkTarget, embedding: str, company_ids: frozenset[int], settings: NewsSettings
) -> LinkDecision:
    """후보 조회 → 부모 선택 → 기록."""

    candidates = fetch_link_candidates(
        cluster_id=target.cluster_id,
        embedding=embedding,
        first_published_at=target.first_published_at,
        company_ids=company_ids,
        lookback_days=settings.issue_link_lookback_days,
        min_score=(
            settings.issue_link_threshold
            if company_ids
            else settings.issue_link_no_company_threshold
        ),
        limit=CANDIDATE_LIMIT,
    )
    decision = choose_parent(
        candidates,
        first_published_at=target.first_published_at,
        company_ids=company_ids,
        threshold=settings.issue_link_threshold,
        no_company_threshold=settings.issue_link_no_company_threshold,
    )
    record_link_decision(target.cluster_id, decision.parent_id, decision.score)

    if decision.parent_id is not None:
        logger.info(
            "[연결] %s '%s' ← 부모 %s (코사인 %.3f)",
            target.cluster_id,
            target.title,
            decision.parent_id,
            decision.score,
        )
    return decision


def run() -> dict[str, int]:
    """스케줄 진입점. 지금부터 lookback 안의 연결 대상을 런당 상한만큼 잇는다.

    NEWS_ISSUE_LINK_ENABLED 가 꺼져 있으면 아무것도 하지 않고 0 통계를 돌려준다.
    """

    settings = get_news_settings()
    if not settings.issue_link_enabled:
        logger.info("[연결] NEWS_ISSUE_LINK_ENABLED=false — 건너뜀")
        return dict.fromkeys(STAT_KEYS, 0)

    return link_pending(
        since=now_kst() - timedelta(days=settings.issue_link_lookback_days),
        limit=settings.issue_link_max_per_run,
    )


def link_pending(since: datetime, limit: int) -> dict[str, int]:
    """first_published_at 이 since 이후인 연결 대상을 limit 개까지 임베딩하고 오래된 순으로 잇는다.

    스위치와 무관하게 돈다. 백필 스크립트가 기간을 넓혀 부른다.
    """

    settings = get_news_settings()
    stats = dict.fromkeys(STAT_KEYS, 0)
    targets = fetch_link_targets(since, limit)
    stats["scanned"] = len(targets)
    if not targets:
        logger.info("[연결] 대상 없음")
        return stats

    embeddings = {target.cluster_id: target.embedding for target in targets if target.embedding}
    failed: set[int] = set()

    missing = [target for target in targets if not target.embedding]
    if missing:
        saved, embed_failed = _embed_missing(missing, settings.issue_embedding_model)
        embeddings.update(saved)
        failed |= embed_failed
        stats["embedded"] = len(saved)

    companies = fetch_cluster_company_ids([target.cluster_id for target in targets])

    for target in targets:
        if target.cluster_id not in embeddings:
            continue
        try:
            decision = _link_one(
                target,
                embeddings[target.cluster_id],
                companies.get(target.cluster_id, frozenset()),
                settings,
            )
        except Exception as e:
            failed.add(target.cluster_id)
            logger.warning(
                "이슈 연결 실패(다음 런에 재시도): cluster_id=%s, %s: %s",
                target.cluster_id,
                type(e).__name__,
                e,
            )
            continue
        stats["roots" if decision.parent_id is None else "linked"] += 1

    stats["failed"] = len(failed)
    logger.info(
        "[연결] 대상 %d → 임베딩 %d, 연결 %d, 루트 %d, 실패 %d",
        stats["scanned"],
        stats["embedded"],
        stats["linked"],
        stats["roots"],
        stats["failed"],
    )

    if stats["failed"] == stats["scanned"]:
        raise RuntimeError(
            f"이슈 연결 전건 실패 ({stats['failed']}건) — Bedrock·DB 장애로 보고 재시도한다"
        )
    return stats
