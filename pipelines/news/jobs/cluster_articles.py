"""클러스터 판정과 승격 — news_cluster_articles DAG 의 assign_clusters·promote_clusters task.

수집 잡(collect_articles)이 본문과 news_companies 까지 채워 저장한 기사 중 cluster_id 가 없는
것을 읽어 판정한다. 이 잡만 클러스터를 쓰므로 수집 DAG 와 백필 DAG 가 동시에 돌아도 같은 사건에
클러스터가 둘 생기지 않는다(DAG 의 max_active_runs=1).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.postgres.news_clusters import (
    count_pending_representatives,
    fetch_cluster_candidates,
    fetch_cluster_seeds,
    fetch_idf_table,
    fetch_promotable_cluster_ids,
    fetch_unclustered_news,
    fetch_untitled_promoted,
    promote_cluster,
    record_assignments,
    update_cluster_title,
)
from pipelines.news.transformers.cluster_titler import Headline, title_clusters
from pipelines.news.transformers.clustering.batch import row_documents, seed_window
from pipelines.news.transformers.clustering.online import assign_online
from pipelines.news.transformers.clustering.representative import pick_representative
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def assign() -> dict[str, int]:
    """미판정 기사를 발행 시각순으로 하나씩 기존 클러스터에 편입시키거나 새 클러스터로 만든다.

    이번 수집 런의 기사만이 아니라 cluster_id 가 없는 기사 전부가 대상이다 — 지난 런에 기록이
    실패한 기사도 다시 판정된다.
    """

    settings = get_news_settings()
    started_at = datetime.now(SEOUL_TIMEZONE)

    rows = fetch_unclustered_news()
    if not rows:
        logger.info("[cluster_articles] 판정 대상 없음")
        return {"articles": 0, "created": 0, "updated": 0, "failed": 0}

    documents, published_ats = row_documents(
        rows, settings.cluster_lead_chars, settings.cluster_description_weight
    )
    window_start, window_end = seed_window(
        published_ats, settings.cluster_window_days, settings.cluster_backward_days
    )
    seeds = fetch_cluster_seeds(window_start, window_end)
    idf = fetch_idf_table(started_at - timedelta(days=settings.cluster_idf_days))

    assignments = assign_online(
        documents,
        published_ats,
        seeds,
        idf,
        threshold=settings.cluster_threshold,
        promote_size=settings.cluster_promote_size,
        window_days=settings.cluster_window_days,
        backward_days=settings.cluster_backward_days,
    )
    result = record_assignments(assignments, rows, documents, settings.cluster_keyword_count)

    joined = sum(1 for assignment in assignments if assignment.seed is not None)
    logger.info(
        "[cluster_articles] 판정: 기사 %d건 → 시드 %d개 중 편입 %d / 신규 %d "
        "(후보 %d건, count 만 %d건), 기록 실패 %d",
        len(rows),
        len(seeds),
        joined,
        len(assignments) - joined,
        sum(len(assignment.candidates) for assignment in assignments),
        sum(len(assignment.followers) for assignment in assignments),
        result["failed"],
    )

    return {"articles": len(rows), **result}


def promote() -> dict[str, int]:
    """후보가 다 찬 클러스터의 대표를 정하고, 대표가 있는데 이름이 없는 클러스터에 이름을 짓는다.

    pending_triples 는 삼중항 추출을 아직 시도하지 않은 대표 기사 수다. DAG 는 이 값이 0 이 아닐
    때만 삼중항 DAG 를 깨운다 — 이번 런에 승격이 없어도 지난 런에 추출이 실패한 대표가 남아
    있으면 다시 깨운다.
    """

    started_at = datetime.now(SEOUL_TIMEZONE)

    promoted = _pick_representatives(started_at)
    titled = _title_clusters(started_at)
    pending = count_pending_representatives(get_news_settings().cluster_promote_size)

    logger.info(
        "[cluster_articles] 완료: 승격 %d, 제목 %d, 삼중항 미처리 대표 %d",
        promoted,
        titled,
        pending,
    )

    return {"promoted": promoted, "titled": titled, "pending_triples": pending}


def _pick_representatives(started_at: datetime) -> int:
    """후보가 다 찼는데 대표가 없는 클러스터의 대표를 정한다. 승격한 클러스터 수를 돌려준다.

    후보의 본문은 수집 때 저장돼 있으므로 크롤링하지 않는다. 후보 전부에 본문이 없는 클러스터
    (본문 없이 저장하던 옛 구조의 행)는 승격하지 않는다.
    """

    settings = get_news_settings()
    since = started_at - timedelta(days=settings.cluster_promote_retry_days)
    cluster_ids = fetch_promotable_cluster_ids(settings.cluster_promote_size, since)
    if not cluster_ids:
        return 0

    candidates = fetch_cluster_candidates(cluster_ids)
    idf = fetch_idf_table(started_at - timedelta(days=settings.cluster_idf_days))

    promoted = 0
    without_body: list[int] = []
    failed: list[int] = []
    for cluster_id in cluster_ids:
        rows = candidates.get(cluster_id, [])
        index = pick_representative(
            [row["title"] for row in rows],
            [row["text"] for row in rows],
            idf,
            settings.cluster_representative_min_chars,
        )
        if index is None:
            without_body.append(cluster_id)
            continue

        try:
            if promote_cluster(cluster_id, rows[index]["_news_id"]):
                promoted += 1
        except Exception as e:
            failed.append(cluster_id)
            logger.error(
                "[cluster_articles] 승격 기록 실패(건너뜀): cluster_id=%s, %s: %s",
                cluster_id,
                type(e).__name__,
                e,
            )

    logger.info(
        "[cluster_articles] 승격: 대상 %d → 승격 %d / 본문 없음 %d / 실패 %d",
        len(cluster_ids),
        promoted,
        len(without_body),
        len(failed),
    )
    if without_body:
        logger.warning(
            "[cluster_articles] 후보에 본문이 하나도 없는 클러스터 %d개: cluster_id=%s",
            len(without_body),
            without_body,
        )

    return promoted


def _title_clusters(started_at: datetime) -> int:
    """승격됐는데 이름이 없는 클러스터에 후보 기사 제목들로 이름을 짓는다. 지은 수를 돌려준다."""

    settings = get_news_settings()
    since = started_at - timedelta(days=settings.cluster_promote_retry_days)
    cluster_ids = fetch_untitled_promoted(settings.cluster_promote_size, since)
    if not cluster_ids:
        return 0

    candidates = fetch_cluster_candidates(cluster_ids)
    titles = title_clusters(
        {
            cluster_id: [
                Headline(title=row["title"], published_at=row["published_at"])
                for row in candidates.get(cluster_id, [])
            ]
            for cluster_id in cluster_ids
        },
        max_concurrency=settings.news_llm_max_concurrency,
        max_chars=settings.cluster_title_max_chars,
    )
    for cluster_id, title in titles.items():
        update_cluster_title(cluster_id, title)

    untitled = [cluster_id for cluster_id in cluster_ids if cluster_id not in titles]
    logger.info(
        "[cluster_articles] 제목: 대상 %d → 생성 %d / 실패 %d",
        len(cluster_ids),
        len(titles),
        len(untitled),
    )
    if untitled:
        logger.warning(
            "[cluster_articles] 제목 생성 실패 %d건: cluster_id=%s", len(untitled), untitled
        )

    return len(titles)
