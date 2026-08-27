# Migrations

DB migration 파일을 관리합니다. 운영 DB에 migration을 적용하기 전에는 반드시 리뷰와 백업 계획을 확인합니다.

마이그레이션 러너는 아직 도입하지 않아 **DB에 수동 적용**합니다. 신규 DB 기준의 베이스라인 DDL이며,
모든 파일은 반복 적용 안전(idempotent — `IF NOT EXISTS` / `CREATE OR REPLACE`)하게 작성합니다.
