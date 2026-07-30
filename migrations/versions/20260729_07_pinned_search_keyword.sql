-- 고정(pinned) 검색 키워드: is_pinned 컬럼 + '특징주' 시드
--
-- 적용 전제: 04_keyword_search 적용 완료. 반복 적용 안전(idempotent).
-- (신규 DB는 04에서 이미 is_pinned 컬럼이 생성되므로 ADD COLUMN은 no-op)
--
-- is_pinned=true 키워드는 fetch_active_search_keywords의 로테이션/배치 제한과
-- 무관하게 매 실행 항상 검색에 포함되고, 향후 삭제/일시정지 로직에서 보호된다.
-- 운영 중 다른 고정 키워드는 다음처럼 승격한다:
--   UPDATE search_keywords SET is_pinned = true WHERE keyword = '<키워드>';

ALTER TABLE search_keywords
    ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT false;

-- '특징주' 고정 키워드 시드. 테마 파생으로 이미 존재하면 pinned로 승격만 한다.
INSERT INTO search_keywords (keyword, source_type, status, is_pinned)
VALUES ('특징주', NULL, 'active', true)
ON CONFLICT (keyword) DO UPDATE
    SET is_pinned = true,
        status = 'active';
