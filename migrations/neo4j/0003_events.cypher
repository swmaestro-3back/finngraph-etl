// EVENT — 뉴스 클러스터 승격 노드 (pipelines/events). 키는 news_clusters.id.
CREATE CONSTRAINT event_cluster_id_unique IF NOT EXISTS
FOR (e:Event) REQUIRE e.cluster_id IS UNIQUE;

// 타임라인 정렬 키
CREATE INDEX event_last_published_at IF NOT EXISTS
FOR (e:Event) ON (e.last_published_at);
