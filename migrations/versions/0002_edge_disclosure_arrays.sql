-- entities_relations 뷰 끝에 공시 근거 배열 2개를 추가한다 — Neo4j 간선에서 근거
-- 공시(원장 rcept_no)와 계약 요약(item)을 RDB 조회 없이 바로 읽기 위한 캐시다.
-- CREATE OR REPLACE VIEW 는 기존 컬럼 뒤에 추가만 허용되므로 DROP 없이 안전하다.
-- 기존 DB에는 initdb가 다시 돌지 않으므로 수동 적용이 필요하다:
--   psql "$DATABASE_URL" -f migrations/versions/0002_edge_disclosure_arrays.sql
-- 적용 후 link_supply_contracts job 을 한 번 돌리면 기존 간선에도 배열이 실린다.

CREATE OR REPLACE VIEW entities_relations AS
SELECT subject_name, subject_type, relation, object_name, object_type,
       count(*) FILTER (WHERE source_type = 'news')       AS news_mention_count,
       count(*) FILTER (WHERE source_type = 'disclosure') AS disclosure_count,
       min(mentioned_at) AS first_mentioned_at,
       max(mentioned_at) AS last_mentioned_at,
       -- item 이 NULL 인 행은 items 에서만 빠진다(Neo4j 리스트 속성은 null 원소 불가).
       array_agg(rcept_no ORDER BY mentioned_at, rcept_no)
           FILTER (WHERE source_type = 'disclosure')      AS disclosure_rcept_nos,
       array_agg(item ORDER BY mentioned_at, rcept_no)
           FILTER (WHERE source_type = 'disclosure' AND item IS NOT NULL)
                                                          AS disclosure_items
FROM relation_sources
GROUP BY 1, 2, 3, 4, 5;
