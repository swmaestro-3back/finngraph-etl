CREATE TABLE IF NOT EXISTS market_days (
    trade_date        DATE PRIMARY KEY,
    is_open           BOOLEAN NOT NULL,
    is_business_day   BOOLEAN NOT NULL,
    is_settlement_day BOOLEAN NOT NULL,
    weekday_code      TEXT    NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stock_calendar_events (
    id          BIGSERIAL PRIMARY KEY,
    event_date  DATE    NOT NULL,
    kind        TEXT    NOT NULL,
    ticker      TEXT    NOT NULL,
    stock_name  TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    source_key  TEXT    NOT NULL,
    basis_date  DATE    NOT NULL,
    end_date    DATE,
    amount      NUMERIC,
    ratio       NUMERIC,
    label       TEXT,
    detail      JSONB   NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_stock_calendar_events UNIQUE (source, source_key, kind),
    CONSTRAINT chk_stock_calendar_events_kind CHECK (kind IN (
        'DIV_EX', 'DIV_RECORD', 'DIV_PAY',
        'BONUS_EX', 'BONUS_LIST',
        'RIGHTS_EX', 'RIGHTS_SUBSCRIBE', 'RIGHTS_LIST',
        'AGM'))
);

CREATE INDEX IF NOT EXISTS idx_stock_calendar_events_ticker_date
    ON stock_calendar_events (ticker, event_date);
CREATE INDEX IF NOT EXISTS idx_stock_calendar_events_date
    ON stock_calendar_events (event_date);
CREATE INDEX IF NOT EXISTS idx_stock_calendar_events_source_basis
    ON stock_calendar_events (source, basis_date);

CREATE TABLE IF NOT EXISTS ipo_offerings (
    ticker        TEXT NOT NULL,
    name          TEXT NOT NULL,
    subscr_start  DATE NOT NULL,
    subscr_end    DATE NOT NULL,
    offer_price   NUMERIC,
    pay_date      DATE,
    refund_date   DATE,
    listing_date  DATE,
    lead_managers TEXT,
    basis_date    DATE NOT NULL,
    detail        JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, subscr_start)
);

CREATE INDEX IF NOT EXISTS idx_ipo_offerings_basis ON ipo_offerings (basis_date);
