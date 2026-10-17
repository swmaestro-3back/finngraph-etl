-- 봉 등락률 ─────────────────────────────────────────────────────────────────
-- 직전 봉 종가 대비 등락률(%). 소수 둘째 자리로 반올림한다(r_1w·r_1m 과 같은 단위).
-- 일봉은 직전 거래일, 주봉·월봉은 직전 주·월 봉이 기준이다. 직전 봉이 없는 첫 봉은 NULL.
-- 값은 봉을 쓰는 DAG 의 등락률 태스크가 적재 뒤에 채운다(여기서는 컬럼만 만든다).
ALTER TABLE stock_candles_daily  ADD COLUMN IF NOT EXISTS change_rate NUMERIC;
ALTER TABLE stock_candles_period ADD COLUMN IF NOT EXISTS change_rate NUMERIC;
ALTER TABLE theme_candles_daily  ADD COLUMN IF NOT EXISTS change_rate NUMERIC;
ALTER TABLE theme_candles_period ADD COLUMN IF NOT EXISTS change_rate NUMERIC;
