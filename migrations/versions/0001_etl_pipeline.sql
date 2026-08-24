-- ETL 파이프라인 스키마 — 0000 베이스라인에 얹는 증분. 반복 적용 안전하다.

-- ── company_financials ──────────────────────────────────────────────────────

ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS eps        NUMERIC;
ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS bps        NUMERIC;
ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- 베이스라인 UNIQUE 를 걷어낸다. 제약 이름이 63자로 잘려 상수로는 빗나가므로 정의로 찾는다.
DO $$
DECLARE
    con_name TEXT;
BEGIN
    SELECT conname INTO con_name
      FROM pg_constraint
     WHERE conrelid = 'company_financials'::regclass
       AND contype = 'u'
       AND pg_get_constraintdef(oid) =
           'UNIQUE (company_id, fiscal_yymm, period_type, fs_div, source)';
    IF con_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE company_financials DROP CONSTRAINT %I', con_name);
    END IF;
END $$;

-- 정정공시는 같은 회계기간에 접수번호만 다른 행으로 온다. KIS 행은 접수번호가 없다.
DROP INDEX IF EXISTS company_financials_uk;
CREATE UNIQUE INDEX IF NOT EXISTS company_financials_uk
    ON company_financials (company_id, source, fs_div, fiscal_yymm, period_type, COALESCE(rcept_no, ''));

CREATE INDEX IF NOT EXISTS company_financials_lookup_idx
    ON company_financials (company_id, period_type, fiscal_yymm DESC);

-- ── 수급 · 배당 ─────────────────────────────────────────────────────────────

ALTER TABLE investor_flows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dividends      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- ── companies ───────────────────────────────────────────────────────────────

-- 설명을 만든 원천 공시. 새 보고서가 없으면 설명을 다시 만들지 않는다.
ALTER TABLE companies ADD COLUMN IF NOT EXISTS description_rcept_no TEXT;

-- ── service_companies ───────────────────────────────────────────────────────
--
-- 수집 대상 목록. 비면 시세·수급·배당·재무 잡의 대상이 전부 0이 된다.
--
-- 테마를 담지 않는다. 답하는 질문이 "이 법인을 수집하는가" 하나뿐이고, 테마 소속은
-- theme_stocks 가 다대다로 이미 들고 있다. 여기에 theme_id 를 두면 두 테마에 걸친
-- 법인이 두 행이 되어 JOIN 한 번에 같은 종목을 두 번 수집한다.

CREATE TABLE IF NOT EXISTS service_companies (
    company_id BIGINT PRIMARY KEY REFERENCES companies (id) ON DELETE CASCADE,
    name       TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
