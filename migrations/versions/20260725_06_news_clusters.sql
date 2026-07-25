-- 클러스터링 결과 저장 (docs/2026-07-24-db-schema.md 기준)
--
-- 계산은 news_relations만으로 가능(triplet GROUP BY) — 이 테이블은 결과 저장·서빙용.
-- 없으면 매 조회마다 재계산되어 클러스터 ID가 실행마다 흔들리고 서빙 성능이 저하된다.
-- 클러스터링 job이 배치로 쓰고, 서빙은 읽기만 한다.
-- 적용 전제: 01_news 적용 완료. 반복 적용 안전.

CREATE TABLE IF NOT EXISTS news_clusters (
    id           BIGSERIAL PRIMARY KEY,
    cluster_key  TEXT NOT NULL,        -- 예: (subject, relation, object) + 시간창 해시
    news_id      BIGINT NOT NULL REFERENCES news (id) ON DELETE CASCADE,
    clustered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cluster_key, news_id)
);

CREATE INDEX IF NOT EXISTS idx_news_clusters_key ON news_clusters (cluster_key);
