-- 이슈 타임라인 ────────────────────────────────────────────────────────────
-- 이슈(이름과 대표 기사가 있는 클러스터)를 같은 이야기의 앞선 이슈에 잇는다(news_cluster_articles 의
-- link_issues task, pipelines/news/jobs/link_issues.py). LLM 은 부르지 않는다.
-- embedding 은 연결 판정용 Titan v2(1024) 벡터다 — 제목, 대표 기사 요약 한 줄, 초기 후보 기사 제목으로 만든다.
-- story_root_id 는 이야기 첫 클러스터 id(루트는 자기 id), parent_cluster_id 는 바로 앞 노드(루트는 NULL),
-- link_score 는 부모와의 코사인이다. link_relation 은 부모와의 관계로, 같은 사건이 클러스터 둘로 갈린
-- 것이면 'same_event'(백엔드가 한 노드로 접는다), 후속 사건이면 'follow_up', 루트는 NULL 이다.
-- linked_at 이 NULL 이면 아직 연결 판정 전이다. 연결 쓰기는 updated_at 을 건드리지 않는다 — 승격·제목
-- 재시도와 Event 스캔이 updated_at 창으로 대상을 고른다.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS embedding vector(1024);
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS story_root_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS parent_cluster_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS link_score NUMERIC;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS link_relation TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;

-- ADD CONSTRAINT 에는 IF NOT EXISTS 가 없어 반복 적용이 깨지지 않게 이름으로 확인한다.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'chk_news_clusters_link_relation'
           AND conrelid = 'news_clusters'::regclass
    ) THEN
        ALTER TABLE news_clusters
            ADD CONSTRAINT chk_news_clusters_link_relation
            CHECK (link_relation IN ('follow_up', 'same_event'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_news_clusters_story_root ON news_clusters (story_root_id);
CREATE INDEX IF NOT EXISTS idx_news_clusters_parent ON news_clusters (parent_cluster_id);
