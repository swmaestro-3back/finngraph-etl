# Migrations

DB migration 파일을 관리합니다. 운영 DB에 migration을 적용하기 전에는 반드시 리뷰와 백업 계획을 확인합니다.

마이그레이션 러너는 아직 도입하지 않아 **DB에 수동 적용**합니다. 신규 DB 기준의 베이스라인 DDL이며,
모든 파일은 반복 적용 안전(idempotent — `IF NOT EXISTS` / `CREATE OR REPLACE`)하게 작성합니다.

## 적용 순서 (FK 의존 순서 — 번호순으로 적용)


| 순서  | 파일                                               | 내용                                                           |
| --- | ------------------------------------------------ | ------------------------------------------------------------ |
| 1   | `versions/20260725_01_news.sql`                  | `news` 본체                                                    |
| 2   | `versions/20260725_02_companies.sql`             | `companies` 기업 마스터 (KRX 적재 예정)                               |
| 3   | `versions/20260725_03_themes.sql`                | `themes` / `theme_companies` / `news_themes`                 |
| 4   | `versions/20260725_04_keyword_search.sql`        | `search_keywords`                                            |
| 5   | `versions/20260725_05_news_relations.sql`        | `news_relations` + 뷰(`news_companies`, `entities_relations`) |
| 6   | `versions/20260729_07_pinned_search_keyword.sql` | `search_keywords.is_pinned` 컬럼 + '특징주' 고정 키워드 시드             |
