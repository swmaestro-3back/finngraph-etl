-- 뉴스 본체 (docs/2026-07-24-db-schema.md 기준)
--
-- 마이그레이션 러너 미도입 → DB에 수동 적용(반복 적용 안전, IF NOT EXISTS).
-- 적용 순서: 01_news → 02_companies → 03_themes → 04_keyword_search
--            → 05_news_relations (FK 의존 순서)

CREATE TABLE IF NOT EXISTS news (
    id                  BIGSERIAL PRIMARY KEY,
    title               TEXT,
    description         TEXT,
    summary             TEXT,
    body_text           TEXT,
    link                TEXT UNIQUE,          -- 중복 판정·UPSERT 키
    originallink        TEXT,
    published_at        TIMESTAMPTZ,
    is_material         BOOLEAN,              -- material 필터 3-상태: NULL=미판정 / TRUE=유지 / FALSE=소프트 삭제
    source_type         VARCHAR(20),          -- headline / keyword_search
    collected_at        TIMESTAMPTZ DEFAULT now(),
    relation_extracted  BOOLEAN               -- 삼중항 추출 3-상태: NULL=미처리(추출 대상) / TRUE=관계 있음 / FALSE=관계 없음
);
