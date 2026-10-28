CREATE INDEX CONCURRENTLY IF NOT EXISTS news_extracted_published_idx ON news (published_at DESC NULLS LAST, id DESC) WHERE triple_extracted;
