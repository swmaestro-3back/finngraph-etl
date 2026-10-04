"""클러스터 당사자 기업 조회. news·news_companies 는 news 도메인이 채우고 여기서는 읽기만 한다."""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope

# 클러스터 후보 기사(cluster_terms 가 있는 행) 중 min_articles 건 이상에 연결된 기업.
# news_companies 는 수집 잡이 제목·본문 판정을 통과한 기업만 담는다. 승격 후 기사의 연결은 세지
# 않는다. 많은 후보에 나온 기업이 먼저다.
SELECT_CLUSTER_COMPANY_IDS_SQL = text(
    """
    SELECT n.cluster_id, nco.company_id
      FROM news n
      JOIN news_companies nco ON nco.news_id = n.id
     WHERE n.cluster_id = ANY(:cluster_ids)
       AND n.cluster_terms IS NOT NULL
     GROUP BY n.cluster_id, nco.company_id
    HAVING COUNT(DISTINCT n.id) >= :min_articles
     ORDER BY n.cluster_id, COUNT(DISTINCT n.id) DESC, nco.company_id;
    """
)


def fetch_cluster_company_ids(cluster_ids: list[int], min_articles: int) -> dict[int, list[int]]:
    """클러스터별 당사자 기업 id(companies.id). 기준을 넘는 기업이 없는 클러스터는 키가 없다."""

    if not cluster_ids:
        return {}

    with session_scope() as session:
        rows = session.execute(
            SELECT_CLUSTER_COMPANY_IDS_SQL,
            {"cluster_ids": cluster_ids, "min_articles": min_articles},
        ).fetchall()

    company_ids: dict[int, list[int]] = defaultdict(list)
    for cluster_id, company_id in rows:
        company_ids[int(cluster_id)].append(int(company_id))
    return dict(company_ids)
