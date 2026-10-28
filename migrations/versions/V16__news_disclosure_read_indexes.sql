CREATE INDEX IF NOT EXISTS news_extracted_cluster_idx ON news (cluster_id) WHERE triple_extracted;
CREATE INDEX IF NOT EXISTS news_cluster_published_idx ON news (cluster_id, published_at);
CREATE INDEX IF NOT EXISTS news_published_at_idx ON news (published_at);
CREATE INDEX IF NOT EXISTS news_clusters_event_idx ON news_clusters (id) WHERE representative_news_id IS NOT NULL AND title IS NOT NULL;
CREATE INDEX IF NOT EXISTS disclosures_ticker_idx ON disclosures (ticker);
CREATE INDEX IF NOT EXISTS disclosures_counterparty_ticker_idx ON disclosures (counterparty_ticker) WHERE counterparty_ticker IS NOT NULL;
CREATE INDEX IF NOT EXISTS disclosures_original_rcept_idx ON disclosures (original_rcept_no, rcept_no);
CREATE INDEX IF NOT EXISTS stock_calendar_events_updated_at_idx ON stock_calendar_events (updated_at);
