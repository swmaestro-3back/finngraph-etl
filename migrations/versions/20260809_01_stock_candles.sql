-- 시세 테이블 — 일봉·기간봉
--
-- 분봉(1분·5분)은 이번 범위에서 제외한다. 장중 5분 주기로 종목당 1회씩 부르는 파이프라인이라
-- 성격이 다르고(운영 시간대·호출 예산·보관 정책), 일봉이 자리 잡은 뒤에 별도로 붙인다.
--
-- 키가 symbol이 아니라 stock_id다. 단축코드는 재사용될 수 있고 상장폐지 후에도 같은 문자열이
-- 남지만, stocks.id는 종목 하나를 영구히 가리킨다(20260804_02_stocks_surrogate_id.sql).
-- symbol → stock_id 변환은 적재 시점에 loader가 한다.
--
-- TimescaleDB 하이퍼테이블을 쓰지 않는다. 로컬 이미지에는 확장이 있지만 운영 DB(관리형
-- Postgres 가능성)에 있다는 보장이 없어, 마이그레이션이 환경에 따라 깨지면 안 된다.
-- 보관 정책은 PK 인덱스를 타는 기간 DELETE로 처리한다(README의 1분봉 60일 / 5분봉 3년).

-- ---------------------------------------------------------------------------
-- 1. 일봉 — 영구 보관
-- ---------------------------------------------------------------------------
--
-- 원천이 둘이다. 과거 구간은 FinanceDataReader로 한 번에 백필하고, 이후는 KIS
-- 기간별시세로 매일 덧붙인다. 같은 (stock_id, trade_date)를 양쪽이 채울 수 있으므로
-- upsert로 덮어쓴다. 수정주가 반영으로 과거 값이 바뀌는 경우도 같은 경로로 갱신된다.

CREATE TABLE IF NOT EXISTS stock_daily_candles (
    stock_id    BIGINT  NOT NULL REFERENCES stocks (id) ON DELETE CASCADE,
    trade_date  DATE    NOT NULL,
    open        NUMERIC NOT NULL,
    high        NUMERIC NOT NULL,
    low         NUMERIC NOT NULL,
    close       NUMERIC NOT NULL,
    volume      BIGINT  NOT NULL,
    trade_value BIGINT,            -- 거래대금(원). FDR은 주지 않아 NULL이 될 수 있다.
    source      TEXT    NOT NULL,  -- 'FDR' | 'KIS'
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, trade_date)
);

-- 특정 날짜의 전 종목 조회(일별 파생 계산·시장 스냅샷)가 두 번째 조회 패턴이다.
CREATE INDEX IF NOT EXISTS stock_daily_candles_date_idx
  ON stock_daily_candles (trade_date);

-- ---------------------------------------------------------------------------
-- 2. 기간봉 — 주봉·월봉
-- ---------------------------------------------------------------------------
--
-- KIS inquire-daily-itemchartprice가 FID_PERIOD_DIV_CODE로 W/M을 직접 준다.
-- 일봉에서 집계하지 않는 이유는 그렇게 하면 상장 이전 구간·거래정지 구간에서
-- 원천과 값이 어긋나기 때문이다. base_date는 해당 주/월의 마지막 거래일이다.

CREATE TABLE IF NOT EXISTS stock_period_candles (
    stock_id    BIGINT  NOT NULL REFERENCES stocks (id) ON DELETE CASCADE,
    period      TEXT    NOT NULL,   -- 'W' | 'M'
    base_date   DATE    NOT NULL,
    open        NUMERIC NOT NULL,
    high        NUMERIC NOT NULL,
    low         NUMERIC NOT NULL,
    close       NUMERIC NOT NULL,
    volume      BIGINT  NOT NULL,
    trade_value BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, period, base_date)
);
