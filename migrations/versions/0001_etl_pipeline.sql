-- ETL 파이프라인 스키마 — 0000 베이스라인에 얹는 증분
--
-- 0000_schema.sql 적용 후에 실행한다. 재무·수급·배당·파생 파이프라인이 요구하는
-- 컬럼과 서비스 대상 목록을 더한다. 반복 적용 안전하다.
--
-- ── company_financials ──────────────────────────────────────────────────────
--
-- eps·bps 를 더한다. PER·PBR 의 분모라 없으면 밸류에이션이 성립하지 않는다.
-- KIS 재무비율 API 가 직접 주는 값이라 계산하지 않는다.
--
-- UNIQUE 에 rcept_no 를 넣는다. DART 정정공시는 같은 회계기간에 접수번호만 다른 행으로
-- 오는데, 접수번호가 키에 없으면 정정본이 원본을 덮어써 이력이 사라진다.
-- KIS 행은 접수번호가 없으므로 COALESCE 로 빈 문자열을 대신 넣는다.
--
-- period_type 에 'QC' 가 추가된다. KIS 분기 재무는 **연초부터의 누적**이라 그대로 쓰면
-- 1분기 EPS 를 1년치로 나누게 되고 PER 이 최대 9배 부푼다(2026-08-18 네이버·토스 11종목
-- 대조 확인). 누적은 QC 로 담고 분기 개별값 Q 는 차분해서 만든다.
--
--     A    연간
--     QC   분기 누적   ← KIS 원본
--     Q    분기 개별   ← QC 에서 차분

ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS eps        NUMERIC;
ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS bps        NUMERIC;
ALTER TABLE company_financials ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- 베이스라인의 UNIQUE 를 걷어낸다. 제약 이름은 Postgres 가 63자로 잘라 만들기 때문에
-- 상수로 적으면 빗나간다(끝이 `fs_di_key` 로 잘린다). 카탈로그에서 찾아 지운다.
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

DROP INDEX IF EXISTS company_financials_uk;
CREATE UNIQUE INDEX IF NOT EXISTS company_financials_uk
    ON company_financials (company_id, source, fs_div, fiscal_yymm, period_type, COALESCE(rcept_no, ''));

CREATE INDEX IF NOT EXISTS company_financials_lookup_idx
    ON company_financials (company_id, period_type, fiscal_yymm DESC);

-- ── companies ───────────────────────────────────────────────────────────────
--
-- 업종명은 FDR KRX-DESC 가 준다. DART 기업개황에는 업종 **코드**만 있고 이름이 없어서
-- industry_code 로는 화면에 쓸 수 없다. 기업 설명 폴백(INDUSTRY_FALLBACK)의 재료이기도 하다.

ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry_name TEXT;

-- ── 수급·배당 ───────────────────────────────────────────────────────────────
--
-- 재수집 시 갱신 시각을 남긴다. 외국인 보유율(foreign_ratio)은 순매수와 원천이 달라
-- (현재가 조회 스냅샷) 최신 거래일 행에만 채워지는데, 언제 채워졌는지 알아야 한다.

ALTER TABLE investor_flows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE dividends      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- ── service_companies ───────────────────────────────────────────────────────
--
-- 1차 MVP 서비스 범위는 **반도체와 2차전지**다. 상장사 2,581 곳을 전부 깊게 수집하면
-- DART 개요 13주 · 재무 26주가 걸린다. 서비스하지 않는 종목까지 채우는 비용이라
-- 대상을 좁히고 주기를 매일로 당겼다.
--
-- **소속은 사람이 정한다. 규칙으로는 안 된다.** 표준산업분류로는 삼성전자가 반도체로
-- 안 잡히고(통신 및 방송 장비 제조업) 장비 3사는 특수 목적용 기계로 흩어진다. 이름
-- 패턴도 못 쓴다 — `소재`로 걸면 크리스탈신소재(기타 금융업)가 딸려 온다.
-- 큐레이션 결과는 규칙이 아니라 데이터라 뷰가 아니라 테이블이다.
--
-- **키는 company_id 다.** DART 가 법인 축이라 corp_code 가 같은 행에 있고, 비상장도
-- 담을 수 있으며(테마에 비상장이 들어가는 경우가 있다), 한 법인이 여러 시장에 상장해도
-- 목록이 종목 수만큼 늘지 않는다.
--
-- **PK 가 (company_id, theme_id) 복합이다.** 361 법인 중 25 곳이 반도체·2차전지 양쪽이다.
-- 그래서 수집 대상 선정은 JOIN 이 아니라 EXISTS 로 해야 한다 — JOIN 하면 두 번 수집한다.

CREATE TABLE IF NOT EXISTS service_companies (
    company_id BIGINT NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    theme_id   BIGINT NOT NULL REFERENCES themes (id) ON DELETE CASCADE,
    name       TEXT,   -- 참고용. 식별자는 company_id 다
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, theme_id)
);

CREATE INDEX IF NOT EXISTS service_companies_theme_idx ON service_companies (theme_id);

-- **목록은 여기서 채우지 않는다.** 어느 법인을 서비스할지는 큐레이션 결과이지 스키마가
-- 아니고, 시드가 stocks·companies 를 조인하므로 신규 DB 에서는 그 테이블이 비어 있어
-- 0건으로 조용히 끝난다.
--
-- ⚠️ **목록이 비면 수집 대상도 0이다.** 시세·수급·배당·재무 잡이 전부 service_companies 를
-- EXISTS 로 걸러 대상을 정한다. 파이프라인을 돌리기 전에 목록을 채워야 한다.
--
--     INSERT INTO themes (name) VALUES ('반도체'), ('2차전지') ON CONFLICT DO NOTHING;
--     INSERT INTO service_companies (company_id, theme_id, name)
--     SELECT s.company_id, t.id, c.name
--       FROM stocks s JOIN companies c ON c.id = s.company_id
--       JOIN themes t ON t.name = '반도체'
--      WHERE s.ticker IN ('005930', '000660', ...);
