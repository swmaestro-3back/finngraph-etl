-- 이슈 타임라인 ────────────────────────────────────────────────────────────
-- 이름이 붙은 클러스터를 같은 이야기의 앞선 클러스터에 잇는다(jobs/link_issues.py).
-- summary 는 titler 가 제목과 같은 호출에서 만든 1~2문장 요약, embedding 은 연결 판정용
-- Titan v2(1024) 벡터다. story_root_id 는 이야기 첫 클러스터 id(루트는 자기 id),
-- parent_cluster_id 는 바로 앞 노드(루트는 NULL), link_score 는 부모와의 코사인,
-- linked_at 이 NULL 이면 아직 연결 판정 전이다.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS summary TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS embedding vector(1024);
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS story_root_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS parent_cluster_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS link_score NUMERIC;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_news_clusters_story_root ON news_clusters (story_root_id);

-- 클러스터 판정 기록 ──────────────────────────────────────────────────────
-- 배치 클러스터 판정을 기사 단위로 남긴다: 어느 클러스터로 갔는지, 신규인지, cap 안에
-- 남았는지(kept), 시드와의 유사도. 판정 품질 점검과 평가셋의 원천이다.
CREATE TABLE IF NOT EXISTS news_cluster_judgments (
  id BIGSERIAL PRIMARY KEY,
  run_at TIMESTAMPTZ NOT NULL,
  link TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  published_at TIMESTAMPTZ,
  cluster_id BIGINT REFERENCES news_clusters (id) ON DELETE SET NULL,
  is_new_cluster BOOLEAN NOT NULL,
  kept BOOLEAN NOT NULL,
  seed_similarity NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_news_cluster_judgments_cluster ON news_cluster_judgments (cluster_id);
CREATE INDEX IF NOT EXISTS idx_news_cluster_judgments_run_at ON news_cluster_judgments (run_at);
