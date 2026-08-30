-- 0001: stocks 지수 편입 플래그 승격 (krx100 / krx300 / kosdaq150)
--
-- raw_attributes 에만 있던 KIS master 필드를 컬럼으로 올린다. 값의 원천은 그대로이고
-- 저장 위치만 바뀌므로 백필이 따로 필요 없다 — 다음 stocks_sync_master 실행의 upsert 가
-- 전 종목을 덮어쓰며 채운다. 그전까지는 DEFAULT false 로 남는다.
--
-- KIS master 필드명이 시장마다 다르다.
--   KOSPI   "KRX100" / "KRX300"                   (KOSDAQ150 필드 없음 → 항상 false)
--   KOSDAQ  "KRX100종목여부" / "KRX300종목여부" / "KOSDAQ150지수여부"
--
-- 신규 DB 는 0000_schema.sql 에 이미 포함돼 있어 이 파일이 no-op 이다.
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS krx100    BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS krx300    BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS kosdaq150 BOOLEAN NOT NULL DEFAULT false;
