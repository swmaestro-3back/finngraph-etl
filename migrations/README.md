# Migrations

DB migration 파일을 관리합니다. 운영 DB에 migration을 적용하기 전에는 반드시 리뷰와 백업 계획을 확인합니다.

마이그레이션 러너는 아직 도입하지 않아 **DB에 수동 적용**합니다. 신규 DB 기준의 베이스라인 DDL이며,
모든 파일은 반복 적용 안전(idempotent — `IF NOT EXISTS` / `CREATE OR REPLACE`)하게 작성합니다.

## 적용 순서 (FK 의존 순서 — 번호순으로 적용)

| 순서  | 파일                                     | 내용                                                                            |
| --- | -------------------------------------- | ----------------------------------------------------------------------------- |
| 1   | `versions/0000_schema.sql`             | 신규 DB 기준 베이스라인 DDL (전체 테이블 · 테마 임베딩 컬럼/hnsw 인덱스 · `relation_sources` 근거 원장 · `entities_relations` 뷰) |
| 2   | `versions/0002_news_insights.sql`      | 이슈 인사이트 결과 (`news_insights` 캐시 원장 · `news_insight_items` 종목별 정규화 행) |
