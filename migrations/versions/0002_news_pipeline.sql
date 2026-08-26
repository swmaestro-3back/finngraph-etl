-- 0002: 뉴스 통합 파이프라인 스키마 재편
--
-- news_relations(트리플 평문 적재)를 폐기하고, Neo4j 간선 출처를 추적하는
-- relation_source, 뉴스-기업 매핑 물리 테이블 news_companies로 대체한다.
-- search_keywords는 통합 파이프라인의 검색 쿼리 원천으로 유지한다.
-- (PRD: docs/prd/2026-08-25-news-pipeline.md)

-- ── 제거 ────────────────────────────────────────────────────────────────────
-- news_relations 기반 VIEW(news_companies, entities_relations)는 CASCADE로 함께 제거된다.
-- (DROP VIEW를 따로 쓰면 이미 마이그레이션돼 news_companies가 테이블인 DB에서
--  타입 불일치 에러가 나 멱등성이 깨진다.)
DROP TABLE IF EXISTS news_relations CASCADE;

-- ── search_keywords 유지 + 통합 파이프라인 검색 쿼리 시드 ───────────────────
-- 이 마이그레이션의 구버전이 테이블을 드랍했으므로 방어적으로 재생성한다
-- (신규 DB는 0000이 이미 만들었으니 no-op).
CREATE TABLE IF NOT EXISTS search_keywords (
    id               BIGSERIAL PRIMARY KEY,
    keyword          TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_searched_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_search_keywords_last_searched
  ON search_keywords (last_searched_at);

INSERT INTO search_keywords (keyword)
VALUES
    ('특징주,공급'), ('특징주,계약')
ON CONFLICT (keyword) DO NOTHING;

-- ── news 상태 컬럼 재편 ─────────────────────────────────────────────────────
-- is_processed: 삼중항 추출 시도 완료 여부. FALSE인 행이 extract_triples 대상.
-- relation_extracted: 삼중항이 1개 이상 나왔는지. is_processed=TRUE일 때만 유의미.
ALTER TABLE news ADD COLUMN IF NOT EXISTS is_processed BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE news SET is_processed = TRUE WHERE relation_extracted IS NOT NULL;
ALTER TABLE news DROP COLUMN IF EXISTS is_material;   -- LLM 필터 단계 제거로 미사용
ALTER TABLE news DROP COLUMN IF EXISTS source_type;   -- 수집 경로 단일화로 미사용

-- cluster_rep_news_id: 같은 런에서 같은 사건(클러스터)으로 묶인 기사들의 대표 뉴스 id.
-- 대표 기사와 단독 기사는 자기 자신을 가리킨다. 런 단위 정보라 런 간 병합은 없다.
ALTER TABLE news ADD COLUMN IF NOT EXISTS cluster_rep_news_id
  BIGINT REFERENCES news (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_news_cluster_rep ON news (cluster_rep_news_id);
CREATE INDEX IF NOT EXISTS idx_news_unprocessed
  ON news (id) WHERE NOT is_processed;

-- ── relation_source: Neo4j 간선 출처 ────────────────────────────────────────
-- 한 행 = (출처 뉴스 × Neo4j 간선). 같은 삼중항이 여러 뉴스에서 나오면 간선은
-- 하나로 MERGE되므로 서로 다른 news_id가 같은 neo4j_id를 공유한다.
CREATE TABLE IF NOT EXISTS relation_source (
    id         BIGSERIAL PRIMARY KEY,
    news_id    BIGINT NOT NULL REFERENCES news (id) ON DELETE CASCADE,
    neo4j_id   TEXT   NOT NULL,   -- Neo4j 간선 elementId
    reason     TEXT,              -- 근거 문장 (evidence)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (news_id, neo4j_id)
);

CREATE INDEX IF NOT EXISTS idx_relation_source_news  ON relation_source (news_id);
CREATE INDEX IF NOT EXISTS idx_relation_source_neo4j ON relation_source (neo4j_id);

-- ── news_companies: 뉴스-기업 매핑 (기존 동명 VIEW 대체) ────────────────────
-- 삼중항의 COMPANY 엔티티를 ticker → companies.id로 해석해 적재한다.
CREATE TABLE IF NOT EXISTS news_companies (
    id         BIGSERIAL PRIMARY KEY,
    news_id    BIGINT NOT NULL REFERENCES news (id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (news_id, company_id)
);

CREATE INDEX IF NOT EXISTS idx_news_companies_news    ON news_companies (news_id);
CREATE INDEX IF NOT EXISTS idx_news_companies_company ON news_companies (company_id);
