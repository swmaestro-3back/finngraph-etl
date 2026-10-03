-- ── entity_gazetteer ────────────────────────────────────────────────────────
--
-- 기업 개체 사전 스냅샷. 본문 표기(alias) 하나가 상장 기업 하나를 가리킨다.
-- companies_sync_gazetteer DAG 가 companies·stocks·company_aliases 에서 매번 전량
-- 재생성하고, pipelines/common/gazetteer.py 가 프로세스당 한 번 읽어 FlashText 매처로 쓴다.
--
-- 여러 기업에 걸리는 별칭은 생성 단계에서 우선순위로 정리하거나 제외하므로 alias 자체가 유일하다.
-- canonical_name 은 생성 시점의 companies.name 이다 — 그래프 노드명·원장 엔드포인트와 같은 값.
CREATE TABLE IF NOT EXISTS entity_gazetteer (
    alias          TEXT        PRIMARY KEY,
    company_id     BIGINT      NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    stock_id       BIGINT      NOT NULL REFERENCES stocks (id) ON DELETE CASCADE,
    ticker         VARCHAR(20) NOT NULL,
    canonical_name TEXT        NOT NULL,
    alias_source   TEXT        NOT NULL,   -- NAME | STOCK_NAME | KIS_MASTER | DART | CURATED
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_gazetteer_company_idx ON entity_gazetteer (company_id);
