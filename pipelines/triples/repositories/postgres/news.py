"""트리플 추출 대상 뉴스(클러스터 대표 기사) 조회와 추출 상태(news.triple_extracted) 기록.

news 행 자체는 news 도메인이 쓰고, 여기서는 추출 상태 칼럼만 갱신한다.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope


def fetch_unprocessed_triple_news_items(promote_size: int) -> list[dict[str, Any]]:
    """
    Triple ETL에서 사용.
    삼중항 추출이 아직 시도되지 않은(triple_extracted IS NULL) 클러스터 대표 기사를 상한 없이
    전량 조회한다. 모든 기사에 본문이 있으므로 대표 여부는 news_clusters 로 가린다.
    후보가 promote_size 건 이상인 클러스터만 본다 — 옛 로직이 만든 작은 클러스터의 대표는 뺀다.
    company_ids 는 수집 단계가 판정해 연결한 기업(news_companies)이고, 엔티티의 원천이다.
    추출 중 예외가 난 뉴스는 NULL로 남아 다음 런에서 자동 재시도된다.
    """

    query = """
        SELECT
            n.id,
            n.text,
            (COALESCE(n.published_at, n.collected_at, now()))::date AS mentioned_at,
            ARRAY(
                SELECT nco.company_id
                  FROM news_companies nco
                 WHERE nco.news_id = n.id
                 ORDER BY nco.company_id
            ) AS company_ids
        FROM news n
        JOIN news_clusters nc ON nc.representative_news_id = n.id
        WHERE n.triple_extracted IS NULL
          AND nc.member_count >= :promote_size
          AND n.text IS NOT NULL
          AND BTRIM(n.text) <> ''
        ORDER BY n.id ASC;
    """

    with session_scope() as session:
        rows = session.execute(text(query), {"promote_size": promote_size}).fetchall()

        return [
            {
                "news_id": int(news_id),
                "text": news_text,
                "mentioned_at": mentioned_at,
                "company_ids": [int(company_id) for company_id in company_ids or []],
            }
            for news_id, news_text, mentioned_at, company_ids in rows
        ]


def mark_triple_extraction_result(
    has_triples_ids: list[int], no_triples_ids: list[int]
) -> dict[str, int]:
    """
    Triple ETL에서 호출.
    삼중항 추출을 시도한 뉴스의 triple_extracted에 삼중항 존재 여부를 마킹한다.
    (NULL=미시도이므로 TRUE/FALSE 어느 쪽이든 "시도 완료"를 겸한다)
    """

    unique_true = sorted({int(news_id) for news_id in has_triples_ids if news_id})
    unique_false = sorted({int(news_id) for news_id in no_triples_ids if news_id})

    if not unique_true and not unique_false:
        return {"true_count": 0, "false_count": 0}

    with session_scope() as session:
        if unique_true:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET triple_extracted = TRUE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_true},
            )

        if unique_false:
            session.execute(
                text(
                    """
                    UPDATE news
                    SET triple_extracted = FALSE
                    WHERE id = ANY(:ids);
                    """
                ),
                {"ids": unique_false},
            )

    return {"true_count": len(unique_true), "false_count": len(unique_false)}
