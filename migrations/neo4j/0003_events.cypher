// EVENT — 뉴스 클러스터 승격 노드 (pipelines/events). 키는 news_clusters.id.
CREATE CONSTRAINT event_cluster_id_unique IF NOT EXISTS
FOR (e:Event) REQUIRE e.cluster_id IS UNIQUE;

// 타임라인 정렬 키
CREATE INDEX event_last_published_at IF NOT EXISTS
FOR (e:Event) ON (e.last_published_at);

// HAS_EVENT 간선 대상 조회 키 (Postgres companies.id 미러)
CREATE INDEX company_company_id IF NOT EXISTS
FOR (c:Company) ON (c.company_id);
