-- 기업 마스터 확장 + 별칭 사전 + 미매칭 로그 (SCRUM-50)
--
-- 기존 companies는 KRX 상장사만 담는 (ticker, name, market) 3컬럼짜리였다. 여기서
-- 국내 상장·비상장 + 외국 기업을 모두 담는 마스터로 확장한다.
--
-- ticker의 NOT NULL을 푸는 이유: 비상장 법인은 ticker가 없다(DART 기준 114,602건).
-- 외국 기업도 식별자 체계가 달라 KRX 단축코드를 갖지 않는다. 자연키가 없으므로
-- 식별은 이미 존재하는 surrogate id(companies.id)로 한다.
--
-- ticker 컬럼은 남기되 역할을 좁힌다. 이 컬럼은 "그 법인의 국내 대표 상장 종목(보통주)
-- 단축코드"이고, 국내 상장 법인의 upsert 키로만 쓴다(비상장·외국은 NULL).
--
-- 종목코드로 무언가를 찾는 조인의 대상은 companies가 아니라 stocks다.
-- theme_companies.stock_code와 news_relations.subject_code에는 우선주·ETF·스팩 코드가
-- 들어올 수 있는데 companies에는 보통주 법인만 있다. companies.ticker로 조인하면
-- 그 종목들이 조용히 누락된다. stocks.symbol은 전량을 갖고 있으므로 그쪽으로 조인하고,
-- 법인이 필요하면 stocks.company_id를 한 번 더 타고 간다.
--   theme_companies.stock_code → stocks.symbol (→ stocks.company_id → companies.id)
-- 20260725_03/05의 "companies.ticker와 조인" 주석은 구현된 동작이 아닌 의도 표기였다.

-- ---------------------------------------------------------------------------
-- 1. companies 확장
-- ---------------------------------------------------------------------------

ALTER TABLE companies ALTER COLUMN ticker DROP NOT NULL;

-- country / is_listed는 NOT NULL이지만 기존 행이 있으므로 DEFAULT로 채운 뒤 DEFAULT를 뗀다.
-- DEFAULT를 남겨두면 외국 기업을 넣을 때 country를 빠뜨려도 조용히 'KR'이 되어버린다.
ALTER TABLE companies ADD COLUMN IF NOT EXISTS country   text    NOT NULL DEFAULT 'KR';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_listed boolean NOT NULL DEFAULT true;
ALTER TABLE companies ALTER COLUMN country   DROP DEFAULT;
ALTER TABLE companies ALTER COLUMN is_listed DROP DEFAULT;

-- 외부 식별자. 자연키가 아니라 참조 컬럼이다.
ALTER TABLE companies ADD COLUMN IF NOT EXISTS corp_code    text;  -- DART 고유번호 8자리 (KR 전용)
ALTER TABLE companies ADD COLUMN IF NOT EXISTS wikidata_qid text;  -- 위키데이터 QID (주로 외국)

-- 기업 개요. SCRUM-121(DART 기업개황) / SCRUM-123(설명 생성)에서 채운다.
ALTER TABLE companies ADD COLUMN IF NOT EXISTS name_eng           text;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS description        text;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS description_source text;  -- DART_LLM | WIKIDATA | INDUSTRY_FALLBACK
ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry_name      text;  -- 업종명. 설명 폴백 원문
ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry_code      text;  -- DART induty_code (표준산업분류)
ALTER TABLE companies ADD COLUMN IF NOT EXISTS ceo_name           text;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS established_on     date;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS fiscal_month       text;  -- 'MM'
ALTER TABLE companies ADD COLUMN IF NOT EXISTS homepage           text;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS address            text;

-- 외부 식별자는 부분 UNIQUE. NULL이 대다수라 전역 UNIQUE는 걸 수 없다.
CREATE UNIQUE INDEX IF NOT EXISTS companies_corp_code_uk
  ON companies (corp_code) WHERE corp_code IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS companies_wikidata_uk
  ON companies (wikidata_qid) WHERE wikidata_qid IS NOT NULL;

-- 외국 기업 적재 이후 country='KR' 필터가 상시 붙는다.
CREATE INDEX IF NOT EXISTS companies_country_idx ON companies (country);

-- ---------------------------------------------------------------------------
-- 2. company_aliases — 이름 → 법인 매핑 (뉴스 정규화용)
-- ---------------------------------------------------------------------------
--
-- alias에 전역 UNIQUE를 걸면 안 된다. 동음이의어가 실재한다 — '대덕'은 자치구,
-- '카프로'는 화합물, '성우'는 일본인 성우로도 쓰인다. 같은 표기가 여러 법인에
-- 붙을 수 있고, 해소는 조회 시점의 문맥으로 한다.
--
-- 티커도 별칭으로 넣는다. 기사에 'NVDA'가 그대로 등장할 수 있다.

CREATE TABLE IF NOT EXISTS company_aliases (
    id         BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    alias      TEXT   NOT NULL,
    lang       TEXT,                 -- 'ko' | 'en' 등. 외국 기업 별칭 구분용(SCRUM-122)
    source     TEXT   NOT NULL,      -- KIS_MASTER | KIS_FOREIGN | WIKIDATA | DART | MANUAL
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (alias, company_id)
);

CREATE INDEX IF NOT EXISTS company_aliases_alias_idx ON company_aliases (alias);
CREATE INDEX IF NOT EXISTS company_aliases_company_idx ON company_aliases (company_id);

-- ---------------------------------------------------------------------------
-- 3. unresolved_entities — 미매칭 표기 로그
-- ---------------------------------------------------------------------------
--
-- 별칭 사전을 미리 완벽하게 만들 수는 없다. 매칭 실패를 쌓아 빈도순으로 보강하는
-- 루프가 필요하다. resolved_company_id가 채워지면 해소된 것으로 본다.

CREATE TABLE IF NOT EXISTS unresolved_entities (
    id                  BIGSERIAL PRIMARY KEY,
    surface             TEXT NOT NULL UNIQUE,   -- 뉴스에서 추출된 표기 (예: '오픈에이아이')
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    hit_count           INT NOT NULL DEFAULT 1,
    resolved_company_id BIGINT REFERENCES companies (id) ON DELETE SET NULL
);

-- 미해소 표기를 빈도순으로 뽑아 사전을 보강하는 것이 이 테이블의 유일한 조회 패턴이다.
CREATE INDEX IF NOT EXISTS unresolved_entities_pending_idx
  ON unresolved_entities (hit_count DESC) WHERE resolved_company_id IS NULL;
