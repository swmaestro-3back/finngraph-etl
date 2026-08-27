-- 시세·수급·배당·밸류에이션 테이블명을 도메인 접두사(stock_) + 시간단위 접미사 규칙으로 통일한다.
-- RENAME은 카탈로그만 바꾸므로 데이터 이동 없이 즉시 끝난다. FK·PK·시퀀스는 자동으로 따라온다.
-- 기존 DB에는 initdb가 다시 돌지 않으므로 수동 적용이 필요하다:
--   psql "$DATABASE_URL" -f migrations/versions/0001_rename_stock_tables.sql
-- IF EXISTS 가드는 이미 적용된 DB에서 재실행해도 실패하지 않게 한다.

ALTER TABLE IF EXISTS daily_candles RENAME TO stock_candles_daily;
ALTER INDEX IF EXISTS daily_candles_date_idx RENAME TO stock_candles_daily_date_idx;

ALTER TABLE IF EXISTS stock_period_candles RENAME TO stock_candles_period;

ALTER TABLE IF EXISTS investor_flows RENAME TO stock_investor_flows;

ALTER TABLE IF EXISTS dividends RENAME TO stock_dividends;

ALTER TABLE IF EXISTS valuation_daily RENAME TO stock_valuations_daily;
