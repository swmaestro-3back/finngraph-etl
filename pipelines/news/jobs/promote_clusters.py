"""task: promote_clusters — news_cluster_articles·news_backfill_cluster_articles DAG.

멤버가 승격 기준(NEWS_CLUSTER_PROMOTE_SIZE)에 닿은 클러스터의 대표를 정하고, 대표가 있는데 이름이
없는 클러스터에 이름을 짓는다. 반환값의 pending_triples 는 삼중항 추출을 아직 시도하지 않은 대표
기사 수다. DAG 는 이 값이 0 이 아닐 때만 삼중항 DAG 를 깨운다 — 이번 런에 승격이 없어도 지난 런에
추출이 실패한 대표가 남아 있으면 다시 깨운다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pipelines.common.logging import get_logger
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.postgres.news import fetch_cluster_member_articles
from pipelines.news.repositories.postgres.news_cluster_idf import load_idf_tables
from pipelines.news.repositories.postgres.news_clusters import (
    count_pending_representatives,
    fetch_promotable_cluster_ids,
    fetch_untitled_promoted,
    promote_cluster,
    update_cluster_title,
)
from pipelines.news.transformers.cluster_titler import Headline, title_clusters
from pipelines.news.transformers.clustering.representative import pick_representative
from pipelines.news.utils.date_utils import SEOUL_TIMEZONE

logger = get_logger(__name__)


def run() -> dict[str, int]:
    settings = get_news_settings()
    started_at = datetime.now(SEOUL_TIMEZONE)

    promoted = _pick_representatives(settings, started_at)
    titled = _title_clusters(settings)
    pending = count_pending_representatives(settings.cluster_promote_size)

    logger.info(
        "[promote_clusters] 승격 %d, 제목 %d, 삼중항 미처리 대표 %d", promoted, titled, pending
    )
    return {"promoted": promoted, "titled": titled, "pending_triples": pending}


def _pick_representatives(settings, started_at: datetime) -> int:
    """멤버가 찼는데 대표가 없는 클러스터의 대표(메도이드)를 정한다. 승격한 클러스터 수를 돌려준다.

    저장된 term_weights 의 all 벡터로 고르므로 본문을 다시 토큰화하지 않는다. 멤버 전부에 본문이
    없는 클러스터는 승격하지 않는다.
    """

    since = started_at - timedelta(days=settings.cluster_promote_retry_days)
    cluster_ids = fetch_promotable_cluster_ids(settings.cluster_promote_size, since)
    if not cluster_ids:
        return 0

    members = fetch_cluster_member_articles(cluster_ids)
    idf_all = load_idf_tables()["all"]

    promoted = 0
    without_body: list[int] = []
    failed: list[int] = []
    for cluster_id in cluster_ids:
        rows = members.get(cluster_id, [])
        index = pick_representative(
            [row["term_weights"] for row in rows],
            [row["body_length"] for row in rows],
            idf_all,
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
                "[promote_clusters] 승격 기록 실패(건너뜀): cluster_id=%s, %s: %s",
                cluster_id,
                type(e).__name__,
                e,
            )

    logger.info(
        "[promote_clusters] 승격: 대상 %d → 승격 %d / 본문 없음 %d / 실패 %d",
        len(cluster_ids),
        promoted,
        len(without_body),
        len(failed),
    )
    if without_body:
        logger.warning(
            "[promote_clusters] 멤버에 본문이 하나도 없는 클러스터 %d개: cluster_id=%s",
            len(without_body),
            without_body,
        )
    return promoted


def _title_clusters(settings) -> int:
    """승격됐는데 이름이 없는 클러스터에 멤버 제목(발행 시각순 앞 NEWS_CLUSTER_TITLE_HEADLINES 건)
    으로 이름을 짓는다. 지은 수를 돌려준다. 실패한 클러스터는 다음 런에 다시 시도된다."""

    cluster_ids = fetch_untitled_promoted(settings.cluster_promote_size)
    if not cluster_ids:
        return 0

    members = fetch_cluster_member_articles(cluster_ids)
    titles = title_clusters(
        {
            cluster_id: [
                Headline(title=row["title"], published_at=row["published_at"])
                for row in members.get(cluster_id, [])[: settings.cluster_title_headlines]
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
        "[promote_clusters] 제목: 대상 %d → 생성 %d / 실패 %d",
        len(cluster_ids),
        len(titles),
        len(untitled),
    )
    if untitled:
        logger.warning(
            "[promote_clusters] 제목 생성 실패 %d건: cluster_id=%s", len(untitled), untitled
        )
    return len(titles)
