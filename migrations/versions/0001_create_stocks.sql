CREATE TABLE IF NOT EXISTS stocks (
  symbol text PRIMARY KEY,
  standard_code text NOT NULL,
  name text NOT NULL,
  market text NOT NULL,
  listed_date date,
  is_active boolean NOT NULL DEFAULT true,
  trading_suspended boolean NOT NULL DEFAULT false,
  under_administration boolean NOT NULL DEFAULT false,
  delisting_trade boolean NOT NULL DEFAULT false,
  preferred_stock boolean NOT NULL DEFAULT false,
  etp boolean NOT NULL DEFAULT false,
  spac boolean NOT NULL DEFAULT false,
  source text NOT NULL DEFAULT 'KIS_MASTER',
  synced_at timestamptz,
  inactive_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
