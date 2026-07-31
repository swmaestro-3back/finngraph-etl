-- 기업 마스터 (docs/2026-07-24-db-schema.md 기준)
--
-- KRX 등에서 적재 예정. 삼중항 정규화 앵커(이름→ticker 사전),
-- theme_companies(stock_code)·시계열(stocks)과 ticker 자연 키로 연결.
-- 반복 적용 안전(IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS companies (
    id         BIGSERIAL PRIMARY KEY,
    ticker     VARCHAR(20) NOT NULL UNIQUE,   -- KRX 단축코드, 자연키
    name       TEXT NOT NULL,                 -- 정규 종목명 (news_relations의 정규명 표기 기준)
    market     VARCHAR(20),                   -- KOSPI / KOSDAQ 등
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 이름→ticker 정규화 조회 경로
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies (name);
