"""배치 클러스터 판정을 기사 단위 기록(news_cluster_judgments 행)으로 바꾼다 — 순수 함수만.

cap 에 걸려 버린 기사까지 남겨, 클러스터 판정을 나중에 기사 단위로 평가할 수 있게 한다.
쓰기는 repositories/news_cluster_judgments.py.

kept 는 "실제로 클러스터 멤버가 됐는가"다: cap 안에 남았고, 이번 런에 news 에 새로 저장됐고,
클러스터 기록까지 성공한 기사만 True. 본문 실패 등으로 저장되지 않은 기사는 cap 탈락과 같이
kept=False 로, 판정된 cluster_id 는 그대로 남긴다 — 어느 클러스터로 판정됐는지가 평가의 핵심이고,
멤버 여부는 news.cluster_id 와 일치해야 하기 때문이다. 둘을 가르는 컬럼은 따로 두지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pipelines.news.transformers.clustering.incremental import ClusterAssignment


@dataclass(frozen=True, slots=True)
class ClusterJudgment:
    """기사 하나의 클러스터 판정 (news_cluster_judgments 한 행)."""

    run_at: datetime
    link: str
    title: str
    description: str | None
    # 판정에 쓴 보도 시각. pubDate 를 못 읽은 기사는 실행 시각이다(batch_documents 와 같다)
    published_at: datetime
    # 시드 합류면 시드 id(기록 실패여도), 새 클러스터면 만들어진 id. 안 만들어졌으면 None
    cluster_id: int | None
    is_new_cluster: bool
    kept: bool
    seed_similarity: float | None  # 시드 합류일 때 시드 프로필과의 유사도


def is_stored_member(item: dict[str, Any]) -> bool:
    """이번 런에 news 에 새로 저장돼 클러스터 멤버가 될 수 있는 기사인가.

    본문 실패로 저장되지 않았거나(_news_id 없음) 이미 있던 행이라 건너뛴 기사는 멤버가 아니다.
    """
    return bool(item.get("_news_id")) and item.get("_save_action") != "skipped_existing"


def build_cluster_judgments(
    items: list[dict[str, Any]],
    assignments: list[ClusterAssignment],
    published_ats: list[datetime],
    cluster_ids: list[int | None],
    run_at: datetime,
) -> list[ClusterJudgment]:
    """판정 결과를 기사마다 한 행으로 편다. 판정된 모든 기사(members)가 대상이다.

    items·published_ats 는 assign_batch 의 문서 인덱스 기준이고, cluster_ids 는 assignments 와
    같은 순서로 record_cluster_assignments 가 기록한 클러스터 id(기록하지 않았거나 실패면 None)다.
    """

    if len(items) != len(published_ats):
        raise ValueError("items 와 published_ats 길이가 다릅니다.")
    if len(assignments) != len(cluster_ids):
        raise ValueError("assignments 와 cluster_ids 길이가 다릅니다.")

    judgments: list[ClusterJudgment] = []
    for assignment, recorded_id in zip(assignments, cluster_ids, strict=True):
        seed = assignment.seed
        # 시드 합류는 기록에 실패해도 어느 클러스터로 판정됐는지 안다. 멤버 여부만 False 가 된다
        cluster_id = seed.cluster_id if seed is not None else recorded_id
        kept = set(assignment.kept)

        for index in assignment.members:
            item = items[index]
            judgments.append(
                ClusterJudgment(
                    run_at=run_at,
                    link=item.get("link") or "",
                    title=item.get("title") or "",
                    description=item.get("description") or None,
                    published_at=published_ats[index],
                    cluster_id=cluster_id,
                    is_new_cluster=seed is None,
                    kept=recorded_id is not None and index in kept and is_stored_member(item),
                    seed_similarity=assignment.seed_similarities.get(index),
                )
            )

    return judgments
