CREATE INDEX CONCURRENTLY IF NOT EXISTS news_id_cluster_idx ON news (id) INCLUDE (cluster_id);
