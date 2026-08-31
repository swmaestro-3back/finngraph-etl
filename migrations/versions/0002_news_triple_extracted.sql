-- 0002: news 삼중항추출 플래그 정리 (relation_extracted → triple_extracted, is_processed 제거)
--
-- 구버전 news 테이블은 처리 상태를 is_processed / relation_extracted 두 컬럼으로 들고 있었다.
-- 삼중항추출 여부는 triple_extracted 하나로 통일한다.
--   relation_extracted → triple_extracted 로 rename (기존 값 보존)
--   is_processed       → drop (triple_extracted IS NULL 이 미처리 상태를 대신한다)
--
-- 신규 DB 는 0000_schema.sql 에 이미 포함돼 있어 이 파일이 no-op 이다.

-- rename 은 IF EXISTS 를 지원하지 않아 DO 블록으로 반복 적용 안전하게 감싼다.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'news' AND column_name = 'relation_extracted')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'news' AND column_name = 'triple_extracted') THEN
        ALTER TABLE news RENAME COLUMN relation_extracted TO triple_extracted;
    END IF;
END $$;

-- 두 컬럼 모두 없던 DB 를 위한 안전망 (rename 이 수행됐다면 no-op)
ALTER TABLE news ADD COLUMN IF NOT EXISTS triple_extracted BOOLEAN;

ALTER TABLE news DROP COLUMN IF EXISTS is_processed;

-- 0000_schema.sql 의 news 인덱스를 구버전 DB 에도 맞춰준다.
CREATE INDEX IF NOT EXISTS idx_news_cluster_rep ON news (cluster_rep_news_id);
