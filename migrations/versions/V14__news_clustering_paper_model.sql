-- V14: 뉴스 클러스터링을 서브벡터 + 시간 피처 가중합 모델(Miranda et al. 2018)로 바꾼다.
-- 기사는 vectorize_articles 가 서브벡터별 TF 토큰 가중치(term_weights)를 한 번 저장하고, 판정은 그 값과
-- IDF 사전 표(news_cluster_idf_*)만 읽는다. 클러스터는 멤버 전체의 TF 합(centroids)과 최신·평균·최고
-- 발행 시각을 든다. 옛 모델의 후보 프로필(news.cluster_terms, news_clusters.term_weights·original_size·
-- cohesion)은 지운다 — 적용 뒤 scripts/recluster_news.py --apply 로 전체를 다시 판정한다.
-- member_count 는 "후보 수(≤ 승격 기준)" 에서 "전체 멤버 수" 로 의미가 바뀐다. 하류 조건
-- member_count >= NEWS_CLUSTER_PROMOTE_SIZE 는 그대로 성립한다.

-- ── news ────────────────────────────────────────────────────────────────────
-- 서브벡터별 토큰 TF 가중치(IDF 곱하기 전): {"title": {token: w}, "body": {token: w}, "entities": {company_id: 1.0}}.
-- all(제목+본문)은 저장하지 않고 title+body 합으로 유도한다. NULL 이면 아직 벡터화 전(판정 대상 아님).
ALTER TABLE news ADD COLUMN IF NOT EXISTS term_weights JSONB;
-- 배정 시점 점수 Γ = Σ 서브벡터 코사인×가중치 + Σ 시간 피처×가중치. 새 클러스터의 첫 기사는 NULL.
ALTER TABLE news ADD COLUMN IF NOT EXISTS cluster_score NUMERIC(12, 6);
-- 배정된 클러스터에 대한 피처 7개 {"title","body","all","entities","last","mean","first"}. 첫 기사는 NULL.
ALTER TABLE news ADD COLUMN IF NOT EXISTS cluster_features JSONB;
ALTER TABLE news DROP COLUMN IF EXISTS cluster_terms;

CREATE INDEX IF NOT EXISTS idx_news_unvectorized ON news (id) WHERE term_weights IS NULL;
CREATE INDEX IF NOT EXISTS idx_news_unclustered ON news (id) WHERE cluster_id IS NULL;

-- ── news_clusters ───────────────────────────────────────────────────────────
-- 서브벡터별 멤버 TF 합 {"title": {...}, "body": {...}, "entities": {...}}. 평균(centroid) = 합 / member_count.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS centroids JSONB NOT NULL DEFAULT '{}'::jsonb;
-- 멤버 평균 발행 시각(시간 피처 n2). first(n3)·last(n1) 와 함께 가우시안 시간 피처의 기준이다.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS mean_published_at TIMESTAMPTZ;
UPDATE news_clusters SET mean_published_at = first_published_at WHERE mean_published_at IS NULL;
ALTER TABLE news_clusters ALTER COLUMN mean_published_at SET NOT NULL;

ALTER TABLE news_clusters DROP CONSTRAINT IF EXISTS chk_news_clusters_size;
ALTER TABLE news_clusters DROP COLUMN IF EXISTS term_weights;
ALTER TABLE news_clusters DROP COLUMN IF EXISTS original_size;
ALTER TABLE news_clusters DROP COLUMN IF EXISTS cohesion;

-- ── IDF 사전 ─────────────────────────────────────────────────────────────────
-- rebuild_idf 가 최근 NEWS_CLUSTER_IDF_DAYS 일의 벡터화된 뉴스 전체를 서브벡터 필드별로 집계해 한 트랜잭션에서
-- 통째로 교체한다. 판정 런은 시작 때 읽어 런 내내 고정한다(논문의 고정 IDF 사전). all 은 title∪body 라
-- title·body 의 df 에서 유도할 수 없어 따로 센다.
CREATE TABLE IF NOT EXISTS news_cluster_idf_terms (
    field TEXT    NOT NULL,   -- title · body · all · entities
    token TEXT    NOT NULL,
    df    INTEGER NOT NULL,   -- 코퍼스에서 이 토큰이 나온 문서 수
    PRIMARY KEY (field, token)
);

CREATE TABLE IF NOT EXISTS news_cluster_idf_meta (
    id            SMALLINT    PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    article_count INTEGER     NOT NULL,  -- 코퍼스 문서 수. 모든 field 공통 분모
    corpus_since  TIMESTAMPTZ NOT NULL,  -- 집계 하한 (COALESCE(published_at, collected_at) >= corpus_since)
    built_at      TIMESTAMPTZ NOT NULL
);
