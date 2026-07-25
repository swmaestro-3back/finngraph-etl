-- 키워드 검색 수집: search_keywords (docs/2026-07-24-db-schema.md 기준)
--
-- 적용 전제: 01_news 적용 완료. 반복 적용 안전.

-- 검색 키워드 마스터/큐: 테마명·기업명에서 자동 파생된 검색어를 담는다.
CREATE TABLE IF NOT EXISTS search_keywords (
    id               BIGSERIAL PRIMARY KEY,
    keyword          TEXT NOT NULL UNIQUE,
    source_type      VARCHAR(20),        -- theme (themes 파생) / company (companies 파생)
    source_id        BIGINT,             -- 폴리모픽 참조(source_type에 따른 원본 id), FK 없음
    status           VARCHAR(20) NOT NULL DEFAULT 'active',   -- active / paused
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_searched_at TIMESTAMPTZ         -- 재검색 로테이션 (NULLS FIRST 정렬로 오래 안 돌린 것 우선)
);

-- active 키워드를 오래 안 돌린 순으로 뽑는 조회 경로.
CREATE INDEX IF NOT EXISTS idx_search_keywords_status_last_searched
    ON search_keywords (status, last_searched_at);
