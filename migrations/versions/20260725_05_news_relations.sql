-- 뉴스별 삼중항 + 유도 뷰 (docs/2026-07-24-db-schema.md 기준)
--
-- 뉴스↔엔티티↔관계 매핑의 실체. 별도 entities 마스터 없이 정규명(+타입)이 식별자,
-- code(ticker/ISO)는 보조 키. 엔티티명 정규화는 삼중항 추출 단계에서 보장되며
-- COMPANY 정규명은 KRX 상장 종목명 표기를 따른다(companies 백필 매칭 전제).
-- code는 LLM 출력이 아니라 저장 시 companies 사전 조회로 채움(적재 전엔 NULL → 백필).
-- 적용 전제: 01_news 적용 완료. 반복 적용 안전.

CREATE TABLE IF NOT EXISTS news_relations (
    id            BIGSERIAL PRIMARY KEY,
    news_id       BIGINT NOT NULL REFERENCES news (id) ON DELETE CASCADE,
    subject_name  TEXT NOT NULL,          -- 정규화된 엔티티명
    subject_type  VARCHAR(20) NOT NULL,   -- COMPANY / GOVERNMENT / COUNTRY / COMMODITY / PRODUCT
    subject_code  VARCHAR(20),            -- COMPANY: KRX ticker, COUNTRY: ISO 코드. 없으면 NULL
    relation      VARCHAR(30) NOT NULL,   -- MATERIAL_RELATION_SCHEMA의 닫힌 어휘 (인수하다, 공급하다, ...)
    object_name   TEXT NOT NULL,
    object_type   VARCHAR(20) NOT NULL,
    object_code   VARCHAR(20),
    extracted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (news_id, subject_name, relation, object_name)   -- 재추출 멱등(ON CONFLICT DO NOTHING)
);

-- 서빙 경로: 기업/국가별 뉴스 조회 (code 우선, 없으면 name)
CREATE INDEX IF NOT EXISTS idx_news_relations_subject_code
    ON news_relations (subject_code) WHERE subject_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_news_relations_object_code
    ON news_relations (object_code) WHERE object_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_news_relations_subject_name ON news_relations (subject_name);
CREATE INDEX IF NOT EXISTS idx_news_relations_object_name  ON news_relations (object_name);

-- 클러스터링 경로: triplet 키 grouping
CREATE INDEX IF NOT EXISTS idx_news_relations_triplet
    ON news_relations (subject_name, relation, object_name);

CREATE INDEX IF NOT EXISTS idx_news_relations_news ON news_relations (news_id);

-- ── 유도 뷰 (물리 테이블 불필요) ─────────────────────────────────────────────

-- 뉴스↔기업 매핑. 한계: 관계가 추출된 뉴스만 포함
-- ("언급만 된 기업 뉴스"까지 필요해지면 물리 테이블로 승격).
CREATE OR REPLACE VIEW news_companies AS
SELECT DISTINCT news_id, subject_name AS company_name, subject_code AS ticker
FROM news_relations WHERE subject_type = 'COMPANY'
UNION
SELECT DISTINCT news_id, object_name, object_code
FROM news_relations WHERE object_type = 'COMPANY';

-- 엔티티 수준 전역 관계(집계). 하나의 subject에 여러 object = 여러 행으로 자연 표현.
-- 조회가 무거워지면 MATERIALIZED VIEW로 승격.
CREATE OR REPLACE VIEW entities_relations AS
SELECT subject_name, subject_type, relation, object_name, object_type,
       count(*)          AS news_count,
       min(extracted_at) AS first_seen,
       max(extracted_at) AS last_seen
FROM news_relations
GROUP BY 1, 2, 3, 4, 5;
