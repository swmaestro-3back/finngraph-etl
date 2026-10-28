ALTER TABLE stock_candles_daily ADD COLUMN IF NOT EXISTS high_52w NUMERIC;
ALTER TABLE stock_candles_daily ADD COLUMN IF NOT EXISTS high_52w_date DATE;
ALTER TABLE stock_candles_daily ADD COLUMN IF NOT EXISTS low_52w NUMERIC;
ALTER TABLE stock_candles_daily ADD COLUMN IF NOT EXISTS low_52w_date DATE;
