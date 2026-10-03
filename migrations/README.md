# Migrations

DB migration 파일을 관리합니다. 운영 DB에 migration을 적용하기 전에는 반드시 리뷰와 백업 계획을 확인합니다.

마이그레이션 러너는 아직 도입하지 않아 **DB에 수동 적용**합니다. 신규 DB 기준의 베이스라인 DDL이며,
모든 파일은 반복 적용 안전(idempotent — `IF NOT EXISTS` / `CREATE OR REPLACE`)하게 작성합니다.

## 구성

- `versions/V{n}__{설명}.sql` — Postgres. 번호 순으로 적용합니다. 새 파일은 마지막 번호 다음으로 추가하고,
  다른 브랜치와 번호가 겹치면 머지 전에 다시 매깁니다.
- `neo4j/{nnnn}_{설명}.cypher` — Neo4j 제약·인덱스.

## 로컬 적용

- Postgres: `docker compose up -d db` 가 **새 볼륨일 때만** `docker/db/initdb` 를 거쳐 `versions/*.sql` 을
  순서대로 실행합니다. 기존 볼륨에 새 버전을 반영하려면 해당 파일을 `psql -f` 로 직접 적용하거나
  `docker compose down -v` 후 다시 띄웁니다.
- Neo4j: `neo4j-init` 컨테이너가 기동할 때마다 `neo4j/*.cypher` 전체를 다시 실행합니다(멱등).

## 배포 순서

코드보다 마이그레이션을 먼저 적용합니다. 테이블·컬럼이 없으면 해당 태스크가 실패하고, 그 태스크가
Asset 을 발행하는 경우 하류 DAG까지 그날 멈춥니다. 버전별 주의 사항(V5 테마 봉, V6 등락률, V9 개체
사전)은 `dags/README.md` 의 "배포 순서" 를 참고하세요.
