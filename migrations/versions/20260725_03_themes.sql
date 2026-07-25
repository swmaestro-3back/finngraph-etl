-- 테마 도메인: themes / theme_companies / news_themes (docs/2026-07-24-db-schema.md 기준)
--
-- 테마 데이터의 진실은 테마 크롤러의 Neo4j지만, 뉴스 서빙("테마 클릭 → 뉴스 리스트")은
-- news_themes ⋈ news 를 SQL로 조인해야 하므로 테마를 RDB에도 미러한다.
-- 적용 전제: 01_news 적용 완료(news FK). 반복 적용 안전.

-- themes: 테마 차원. name을 자연키로 upsert하고 id(BIGSERIAL)는 안정적으로 유지된다
--         (Neo4j UUID는 매일 full-refresh로 바뀌므로 쓰지 않는다). news_themes가 이 id를 참조.
CREATE TABLE IF NOT EXISTS themes (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ             -- 미러 시 갱신. stale 테마 감지용(하드 삭제는 안 함)
);

-- theme_companies: 테마↔기업. 크롤러가 검증한 ticker(=stock_code)를 자연키로 저장.
--                  companies와는 stock_code = companies.ticker 자연 키 조인 (별도 FK 없음).
CREATE TABLE IF NOT EXISTS theme_companies (
    id          BIGSERIAL PRIMARY KEY,
    theme_id    BIGINT NOT NULL REFERENCES themes (id) ON DELETE CASCADE,
    stock_code  VARCHAR(20) NOT NULL,   -- ticker → companies.ticker와 조인
    reason      TEXT,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (theme_id, stock_code)
    -- [예정] stock_name TEXT — 크롤러 수집 종목명 보존(company 파생 검색어·정규화 사전용)
);

CREATE INDEX IF NOT EXISTS idx_theme_companies_theme ON theme_companies (theme_id);
CREATE INDEX IF NOT EXISTS idx_theme_companies_stock_code ON theme_companies (stock_code);

-- news_themes: 뉴스↔테마 N:M (테마별 뉴스 서빙의 물리 테이블).
-- link_source: 연결 생성 경로 — keyword_search(현재) / company_membership(예정).
-- 두 경로에서 동시에 걸려도 UNIQUE로 한 행(먼저 기록된 link_source 유지).
CREATE TABLE IF NOT EXISTS news_themes (
    id          BIGSERIAL PRIMARY KEY,
    news_id     BIGINT NOT NULL REFERENCES news (id) ON DELETE CASCADE,
    theme_id    BIGINT NOT NULL REFERENCES themes (id) ON DELETE CASCADE,
    link_source VARCHAR(30),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (news_id, theme_id)
);

-- 테마별 뉴스 조회가 주 경로라 theme_id 인덱스 필수.
CREATE INDEX IF NOT EXISTS idx_news_themes_theme ON news_themes (theme_id);
CREATE INDEX IF NOT EXISTS idx_news_themes_news ON news_themes (news_id);
