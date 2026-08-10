-- companies: ticker 전역 UNIQUE 해제 + 상장폐지 시점 (반복 적용 안전)
--
-- 종목코드가 상장폐지 후 재사용되면 ticker 전역 UNIQUE로는 옛 법인과 새 법인이 공존할 수
-- 없다. "활성 법인 중에서만 유일"로 바꿔 상장폐지된 행이 같은 ticker를 그대로 갖게 한다.
--
-- ticker는 비상장·외국 기업에서 NULL이므로 자연키가 될 수 없다. companies의 키는 이미
-- 있는 surrogate id(BIGSERIAL)이고, ticker는 조회 편의 컬럼이다. theme_companies.stock_code
-- 와 news_relations.subject_code가 ticker 자연키로 조인하고 있어 당장 걷어내지 않는다.
-- 두 도메인이 company_id 조인으로 넘어가면 이 컬럼은 제거 대상이다.
--
-- 주의: 이 변경은 SCRUM-100(주요 RDB스키마 정의)과 범위가 겹친다. 적용 전 조율이 필요하다.

ALTER TABLE companies ADD COLUMN IF NOT EXISTS delisted_at date;

-- 전역 UNIQUE 제거. 제약 이름은 Postgres가 UNIQUE 컬럼에 자동 부여한 기본형.
ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_ticker_key;

CREATE UNIQUE INDEX IF NOT EXISTS companies_active_ticker_uk
  ON companies (ticker) WHERE delisted_at IS NULL;
