# Migrations

DB migration 파일을 관리합니다. 운영 DB에 migration을 적용하기 전에는 반드시 리뷰와 백업 계획을 확인합니다.

마이그레이션 러너는 아직 도입하지 않아 **DB에 수동 적용**합니다. 신규 DB 기준의 베이스라인 DDL이며,
모든 파일은 반복 적용 안전(idempotent — `IF NOT EXISTS` / `CREATE OR REPLACE`)하게 작성합니다.

## 적용 순서 (FK 의존 순서 — 번호순으로 적용)

| 순서 | 파일 | 내용 |
|---|---|---|
| 1 | `versions/20260725_01_news.sql` | `news` 본체 |
| 2 | `versions/20260725_02_companies.sql` | `companies` 기업 마스터 (KRX 적재 예정) |
| 3 | `versions/20260725_03_themes.sql` | `themes` / `theme_companies` / `news_themes` |
| 4 | `versions/20260725_04_keyword_search.sql` | `search_keywords` |
| 5 | `versions/20260725_05_news_relations.sql` | `news_relations` + 뷰(`news_companies`, `entities_relations`) |
| 6 | `versions/20260725_06_news_clusters.sql` | `news_clusters` (클러스터링 결과 저장) |
| 7 | `versions/20260729_07_pinned_search_keyword.sql` | `search_keywords.is_pinned` 컬럼 + '특징주' 고정 키워드 시드 |
| 8 | `versions/0001_create_stocks.sql` | `stocks` 종목 마스터. 날짜 접두사 규칙 이전에 만들어져 파일명만 다르다 |
| 9 | `versions/20260730_01_stocks_raw_attributes.sql` | `stocks.raw_attributes` (필드 탐색·파싱 검증용) |
| 10 | `versions/20260804_01_stocks_share_columns.sql` | `stocks.listed_shares` / `par_value` / `capital` |
| 11 | `versions/20260804_02_stocks_surrogate_id.sql` | `stocks` surrogate id 전환 + `company_id` FK. `companies`(2번) 이후여야 한다 |
| 12 | `versions/20260804_03_companies_active_ticker.sql` | `companies.delisted_at` + 활성 ticker 부분 UNIQUE |
| 13 | `versions/20260808_01_companies_master.sql` | `companies` 확장(국내 비상장·외국 수용) + `company_aliases` + `unresolved_entities` |