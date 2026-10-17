-- 이슈 연결 투표 판정 ─────────────────────────────────────────────────────
-- link_issues task(pipelines/news/jobs/link_issues.py)가 LLM 투표로 이슈를 잇는다. 연결 결과는
-- V12 에서 추가한 news_clusters 열(parent_cluster_id, link_relation, link_score, linked_at)에 쓰고,
-- link_score 에는 사건 유사도(event_score)와 내용 유사도(코사인) 가운데 큰 값을 쓴다.
--
-- issue_kind 는 이슈 성격이다. 'event' 는 회사 자신에게 일어난 사건이며(공시·실적·계약·인수합병·소송
-- 등), 주가 반응과 함께 보도돼도 event 다. 'market_reaction' 은 외부 요인·테마·전망에 따른 주가
-- 반응이며, 이런 이슈는 부모도 후보도 되지 않는다. NULL 은 아직 분류하지 않았다는 뜻이다.
-- 분류를 쓸 때는 updated_at 을 바꾸지 않는다. 승격·제목 재시도와 Event 스캔이 updated_at 범위로
-- 대상을 고르기 때문이다.
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS issue_kind TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS issue_kind_reason TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS issue_kind_model TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS issue_kind_prompt_version TEXT;
ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS issue_kind_at TIMESTAMPTZ;

-- ADD CONSTRAINT 에는 IF NOT EXISTS 가 없으므로, 다시 적용해도 실패하지 않도록 제약 이름으로 확인한다.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'chk_news_clusters_issue_kind'
           AND conrelid = 'news_clusters'::regclass
    ) THEN
        ALTER TABLE news_clusters
            ADD CONSTRAINT chk_news_clusters_issue_kind
            CHECK (issue_kind IN ('event', 'market_reaction'));
    END IF;
END $$;

-- 판정을 실행할 때마다 대상 B 와 후보 A 쌍 하나에 한 행씩 투표 결과를 남긴다. 묻지 않은 투표자의
-- 표는 NULL 이다(확인자는 제안자가 받아들인 쌍에만 묻는다). event_score 는 확인자에게 넘어간
-- 쌍에서만 구한다. accepted 는 judge_vote AND check_vote AND (pair_vote OR rank_vote) 이고,
-- chosen 은 그중 부모로 고른 쌍이다.
-- evidence 에는 단계별 라벨과 코드 점검 사유를 담는다.
CREATE TABLE IF NOT EXISTS news_issue_link_votes (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               UUID             NOT NULL,
    cluster_id           BIGINT           NOT NULL REFERENCES news_clusters (id) ON DELETE CASCADE,
    candidate_cluster_id BIGINT           NOT NULL REFERENCES news_clusters (id) ON DELETE CASCADE,
    cosine               DOUBLE PRECISION NOT NULL,
    event_score          DOUBLE PRECISION,
    gap_hours            DOUBLE PRECISION NOT NULL,
    pair_vote            BOOLEAN,
    rank_vote            BOOLEAN,
    judge_vote           BOOLEAN,
    check_vote           BOOLEAN,
    pair_route           SMALLINT,
    judge_by             TEXT,
    pair_label           TEXT,
    rank_label           TEXT,
    judge_label          TEXT,
    judge_a_scope        TEXT,
    judge_b_scope        TEXT,
    check_role           TEXT,
    vote_count           SMALLINT         NOT NULL,
    accepted             BOOLEAN          NOT NULL,
    chosen               BOOLEAN          NOT NULL,
    evidence             JSONB            NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT uq_news_issue_link_votes UNIQUE (run_id, cluster_id, candidate_cluster_id),
    CONSTRAINT chk_news_issue_link_votes_judge_by CHECK (judge_by IN ('scope_judge', 'plan_reader'))
);

CREATE INDEX IF NOT EXISTS idx_news_issue_link_votes_cluster
    ON news_issue_link_votes (cluster_id, created_at);

-- LLM 응답 캐시이며 감사 기록으로도 쓴다. cache_key 는 sha256(모델|단계|프롬프트 버전|호출 번호|요청
-- 전체)이고, 뉴스 텍스트가 담긴 요청 원문은 저장하지 않는다. 입력이 같으면 다음 실행(재판정 포함)도
-- 이 응답을 그대로 쓴다. response 에는 모델 출력(본문, 도구 입력, 종료 사유, 사용량)을 담는다.
-- cluster_id 는 이 응답을 처음 받은 대상 이슈이고(A 만 읽는 계획 추출에서는 A), candidate_cluster_id
-- 는 쌍 단계에서 본 후보 A 다.
CREATE TABLE IF NOT EXISTS news_issue_link_llm_calls (
    cache_key            TEXT        PRIMARY KEY,
    stage                TEXT        NOT NULL,
    model                TEXT        NOT NULL,
    prompt_version       TEXT        NOT NULL,
    sample               SMALLINT    NOT NULL DEFAULT 0,
    cluster_id           BIGINT      NOT NULL REFERENCES news_clusters (id) ON DELETE CASCADE,
    candidate_cluster_id BIGINT      REFERENCES news_clusters (id) ON DELETE CASCADE,
    response             JSONB       NOT NULL,
    input_tokens         INT         NOT NULL DEFAULT 0,
    output_tokens        INT         NOT NULL DEFAULT 0,
    latency_ms           INT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_news_issue_link_llm_calls_cluster
    ON news_issue_link_llm_calls (cluster_id);
CREATE INDEX IF NOT EXISTS idx_news_issue_link_llm_calls_created
    ON news_issue_link_llm_calls (created_at);
