CREATE TABLE IF NOT EXISTS ipo_filings (
    corp_code         TEXT PRIMARY KEY,
    corp_name         TEXT NOT NULL,
    corp_cls          TEXT NOT NULL,
    status            TEXT NOT NULL,
    spac              BOOLEAN NOT NULL DEFAULT false,
    first_rcept_no    TEXT NOT NULL,
    first_filed_on    DATE NOT NULL,
    latest_rcept_no   TEXT NOT NULL,
    latest_report_nm  TEXT NOT NULL,
    subscr_start      DATE,
    subscr_end        DATE,
    pay_date          DATE,
    offer_price       NUMERIC,
    offer_shares      BIGINT,
    offer_amount      NUMERIC,
    offer_method      TEXT,
    underwriters      JSONB NOT NULL DEFAULT '[]'::jsonb,
    fund_uses         JSONB NOT NULL DEFAULT '[]'::jsonb,
    sellers           JSONB NOT NULL DEFAULT '[]'::jsonb,
    putback           JSONB,
    description_ready BOOLEAN NOT NULL DEFAULT false,
    ticker            TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_ipo_filings_status CHECK (status IN ('FILED', 'PRICED', 'WITHDRAWN'))
);

CREATE INDEX IF NOT EXISTS idx_ipo_filings_subscr ON ipo_filings (subscr_start);

CREATE TABLE IF NOT EXISTS capital_decisions (
    rcept_no            TEXT PRIMARY KEY,
    corp_code           TEXT NOT NULL,
    ticker              TEXT,
    kind                TEXT NOT NULL,
    decided_on          DATE NOT NULL,
    method              TEXT,
    new_shares          BIGINT,
    shares_before       BIGINT,
    fund_uses           JSONB NOT NULL DEFAULT '[]'::jsonb,
    bonus_new_shares    BIGINT,
    bonus_per_share     NUMERIC,
    bonus_record_date   DATE,
    bonus_listing_date  DATE,
    short_sale_from     DATE,
    short_sale_to       DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_capital_decisions_kind CHECK (kind IN ('RIGHTS', 'BONUS', 'BOTH'))
);

CREATE INDEX IF NOT EXISTS idx_capital_decisions_ticker ON capital_decisions (ticker, decided_on);
