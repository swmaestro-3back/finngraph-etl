ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS title TEXT;

CREATE TABLE IF NOT EXISTS search_history (
    company_id       BIGINT PRIMARY KEY REFERENCES companies (id) ON DELETE CASCADE,
    last_searched_at TIMESTAMPTZ NOT NULL
);
