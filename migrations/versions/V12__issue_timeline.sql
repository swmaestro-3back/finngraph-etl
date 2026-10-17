-- 이슈 타임라인 ────────────────────────────────────────────────────────────
-- 이슈(이름과 대표 기사가 있는 클러스터)를 같은 타임라인의 앞선 이슈에 잇는다
-- (pipelines/news/jobs/link_issues.py).
-- embedding 은 연결 판정에 쓰는 Titan v2(1024) 벡터로, 제목, 대표 기사의 한 줄 요약, 멤버 기사 제목
-- 몇 개로 만든다. 기사 제목은 후보 기사(승격 전에 들어온 기사)부터 넣는다.
-- story_root_id 는 타임라인 루트(부모가 없는 타임라인 첫 이슈)의 id 이고, 루트 자신은 자기 id 를
-- 갖는다. parent_cluster_id 는 부모 이슈의 id(루트는 NULL)다. link_score 는 부모와의 코사인
-- 유사도다. link_relation 은 부모와의 관계로, 같은 사건이 클러스터 둘로 나뉜 것이면
-- 'same_event'(백엔드가 한 노드로 합친다), 후속 사건이면 'follow_up', 루트면 NULL 이다.
-- linked_at 이 NULL 이면 아직 연결 판정 전이다. 승격·제목 재시도와 Event 스캔이 updated_at 범위로
-- 대상을 고르므로, 연결을 쓸 때는 updated_at 을 건드리지 않는다.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS embedding vector(1024);
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS story_root_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS parent_cluster_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS link_score NUMERIC;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS link_relation TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;

-- ADD CONSTRAINT 는 IF NOT EXISTS 를 지원하지 않으므로, 다시 적용해도 실패하지 않게 제약
-- 이름으로 존재 여부를 확인한다.
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
