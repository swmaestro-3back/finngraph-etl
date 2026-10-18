"""news_cluster_idf_terms · news_cluster_idf_meta 읽기·쓰기 — 클러스터 판정용 고정 IDF 사전.

rebuild_idf_tables 가 코퍼스(since 이후, term_weights 있는 뉴스 전체)를 서브벡터 필드별로 집계해 한
트랜잭션에서 통째로 교체한다. 판정 런은 load_idf_tables 로 시작 때 한 번 읽어 런 내내 고정한다.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.news.transformers.clustering.document import FIELDS, STORED_FIELDS
from pipelines.news.transformers.clustering.vectorize import IdfTable, IdfTables

_CORPUS_WHERE = """
     WHERE n.term_weights IS NOT NULL
       AND COALESCE(n.published_at, n.collected_at) >= :since
"""

DELETE_TERMS_SQL = text("DELETE FROM news_cluster_idf_terms;")

# 저장 필드(title·body·entities) 하나의 토큰별 문서 수. 키 집합이라 한 문서에서 토큰은 한 번만 센다.
INSERT_FIELD_SQL = text(
    f"""
    INSERT INTO news_cluster_idf_terms (field, token, df)
    SELECT :field, term.token, COUNT(*)
      FROM news n
     CROSS JOIN LATERAL jsonb_object_keys(
           COALESCE(n.term_weights -> CAST(:field AS text), CAST('{{}}' AS jsonb))
         ) AS term(token)
    {_CORPUS_WHERE}
     GROUP BY term.token;
    """
)

# all = title ∪ body. UNION 이 문서 안에서 중복을 지우므로 한 문서는 토큰마다 1 이다.
INSERT_ALL_SQL = text(
    f"""
    INSERT INTO news_cluster_idf_terms (field, token, df)
    SELECT 'all', term.token, COUNT(*)
      FROM news n
     CROSS JOIN LATERAL (
           SELECT jsonb_object_keys(COALESCE(n.term_weights -> 'title', CAST('{{}}' AS jsonb)))
           UNION
           SELECT jsonb_object_keys(COALESCE(n.term_weights -> 'body', CAST('{{}}' AS jsonb)))
         ) AS term(token)
    {_CORPUS_WHERE}
     GROUP BY term.token;
    """
)

COUNT_DOCUMENTS_SQL = text(f"SELECT COUNT(*) FROM news n {_CORPUS_WHERE};")

UPSERT_META_SQL = text(
    """
    INSERT INTO news_cluster_idf_meta (id, article_count, corpus_since, built_at)
    VALUES (1, :article_count, :since, now())
    ON CONFLICT (id) DO UPDATE
       SET article_count = EXCLUDED.article_count,
           corpus_since = EXCLUDED.corpus_since,
           built_at = EXCLUDED.built_at;
    """
)

SELECT_META_SQL = text("SELECT article_count FROM news_cluster_idf_meta WHERE id = 1;")
SELECT_TERMS_SQL = text("SELECT field, token, df FROM news_cluster_idf_terms;")


def rebuild_idf_tables(since: datetime) -> dict[str, int]:
    """표를 지우고 코퍼스를 다시 집계한다. 한 트랜잭션이라 실패하면 이전 표가 남는다."""

    terms = 0
    with session_scope() as session:
        session.execute(DELETE_TERMS_SQL)
        for field in STORED_FIELDS:
            terms += session.execute(INSERT_FIELD_SQL, {"field": field, "since": since}).rowcount
        terms += session.execute(INSERT_ALL_SQL, {"since": since}).rowcount
        article_count = int(session.execute(COUNT_DOCUMENTS_SQL, {"since": since}).scalar_one())
        session.execute(UPSERT_META_SQL, {"article_count": article_count, "since": since})
    return {"terms": terms, "documents": article_count}


def load_idf_tables() -> IdfTables:
    """FIELDS 네 필드의 IdfTable. 표가 없으면(최초) 모두 빈 표 — 모든 토큰이 df=0 이다."""

    with session_scope() as session:
        meta = session.execute(SELECT_META_SQL).fetchone()
        if meta is None:
            return {field: IdfTable() for field in FIELDS}
        article_count = int(meta[0])
        rows = session.execute(SELECT_TERMS_SQL).fetchall()

    frequencies: dict[str, dict[str, int]] = {field: {} for field in FIELDS}
    for field, token, df in rows:
        frequencies.setdefault(field, {})[token] = int(df)
    return {
        field: IdfTable(document_frequency=frequencies[field], article_count=article_count)
        for field in FIELDS
    }
