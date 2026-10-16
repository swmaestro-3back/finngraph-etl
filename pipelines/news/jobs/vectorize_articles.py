"""task: vectorize_articles — news_cluster_articles·news_backfill_cluster_articles DAG.

term_weights 가 없는 기사 전량에 서브벡터별 TF 토큰 가중치(title·body·entities)를 채운다. 본문
전체를 Kiwi 로 토큰화하는 가장 비싼 단계라 기사당 한 번만 한다. 클러스터 판정(assign_clusters)은
이 값만 읽고, IDF 사전 재구축(rebuild_idf)은 이 값이 있는 기사 전체를 코퍼스로 센다. entities 는
news_companies 의 company_id 다.

기사 단위로 격리한다 — 실패한 기사는 term_weights NULL 로 남아 다음 런에 다시 시도되고 그동안 판정
대상에서 빠진다. 토큰화는 커넥션을 열기 전에 끝내고 적재만 짧은 트랜잭션으로 한다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.news.repositories.postgres.news import fetch_unvectorized_news, save_term_weights
from pipelines.news.transformers.clustering.document import TermWeights, build_term_weights

logger = get_logger(__name__)

# 커넥션 하나로 묶어 쓰는 기사 수. 기사마다 SAVEPOINT 라 실패는 그 기사만 되돌린다
WRITE_CHUNK = 500


def run() -> dict[str, int]:
    rows = fetch_unvectorized_news()
    if not rows:
        logger.info("[vectorize_articles] 대상 없음")
        return {"vectorized": 0, "failed": 0}

    vectorized = failed = 0
    for start in range(0, len(rows), WRITE_CHUNK):
        weights: list[tuple[int, TermWeights]] = []
        for row in rows[start : start + WRITE_CHUNK]:
            try:
                weights.append(
                    (
                        row["_news_id"],
                        build_term_weights(row["title"], row["text"], row["company_ids"]),
                    )
                )
            except Exception as e:
                failed += 1
                logger.error(
                    "[vectorize_articles] 토큰화 실패(건너뜀): news_id=%s %s: %s",
                    row["_news_id"],
                    type(e).__name__,
                    e,
                )
        saved = save_term_weights(weights)
        vectorized += saved["saved"]
        failed += saved["failed"]

    logger.info(
        "[vectorize_articles] 기사 %d건 → 벡터화 %d / 실패 %d", len(rows), vectorized, failed
    )
    return {"vectorized": vectorized, "failed": failed}
