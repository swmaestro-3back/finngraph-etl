-- 테마 지수 일봉 ─────────────────────────────────────────────────────────────
-- 구성 종목 시가총액 가중(종목당 25% 상한) 체인 링크 지수. 기준값 1000.
-- 시총은 직전 거래일 종가 × stocks.listed_shares 로 직접 계산한다(stock_valuations_daily 미사용).
CREATE TABLE IF NOT EXISTS theme_candles_daily (
    theme_id    BIGINT  NOT NULL REFERENCES themes (id) ON DELETE CASCADE,
    trade_date  DATE    NOT NULL,
    open        NUMERIC NOT NULL,
    high        NUMERIC NOT NULL,
    low         NUMERIC NOT NULL,
    close       NUMERIC NOT NULL,
    volume      BIGINT  NOT NULL,   -- 참여 종목 거래량 합
    trade_value BIGINT,             -- 참여 종목 거래대금 합
    source      TEXT    NOT NULL,   -- 'CALC'
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (theme_id, trade_date)
);

CREATE INDEX IF NOT EXISTS theme_candles_daily_date_idx ON theme_candles_daily (trade_date);

-- 테마 지수 주봉·월봉 ────────────────────────────────────────────────────────
-- theme_candles_daily 를 집계한 값. base_date 는 구간 시작일(주: 월요일, 월: 1일)로
-- stock_candles_period 와 같은 규칙이다.
CREATE TABLE IF NOT EXISTS theme_candles_period (
    theme_id    BIGINT  NOT NULL REFERENCES themes (id) ON DELETE CASCADE,
    period      TEXT    NOT NULL,   -- 'W' | 'M'
    base_date   DATE    NOT NULL,
    open        NUMERIC NOT NULL,
    high        NUMERIC NOT NULL,
    low         NUMERIC NOT NULL,
    close       NUMERIC NOT NULL,
    volume      BIGINT  NOT NULL,
    trade_value BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (theme_id, period, base_date)
);
