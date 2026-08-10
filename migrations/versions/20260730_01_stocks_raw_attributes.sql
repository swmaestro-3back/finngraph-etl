-- 종목 master 전체 속성 보존 컬럼 (반복 적용 안전)
--
-- KIS master 파일의 모든 필드를 공식 필드명 키의 JSONB로 보존한다.
--
-- 주의: 이력 보존용이 아니다. 동기화마다 통째로 덮어쓰므로 최신 스냅샷만 남는다.
-- 실제 용도는 두 가지다.
--   1) 필드 탐색 — 실제 값을 보고 무엇을 정식 컬럼으로 승격할지 판단한다.
--   2) 파싱 검증 — 고정폭 오프셋이 밀리면 값이 눈에 띄게 이상해져 바로 드러난다.
-- 자주 조회하는 필드는 컬럼으로 승격한다(20260804_01_stocks_share_columns.sql 참고).
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS raw_attributes jsonb;
