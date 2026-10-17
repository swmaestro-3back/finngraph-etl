# Database Image

로컬 DB는 공식 `pgvector/pgvector:pg16` 이미지를 그대로 사용합니다.
별도 커스텀 빌드는 없습니다.

확장은 init SQL로 활성화됩니다:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

현재 pgvector 컬럼은 없습니다. 테마 임베딩은 Neo4j 의 테마 노드·`BELONGS_TO` 간선에 저장합니다.
확장 활성화는 베이스라인(`V1__baseline.sql`)과의 호환을 위해 남아 있습니다.

`initdb/002_run_migrations.sh` 가 `migrations/versions/*.sql` 을 순서대로 실행합니다. 이 디렉토리의
스크립트는 **볼륨이 비어 있을 때 한 번만** 돌므로, 이후 추가된 마이그레이션은 직접 적용해야 합니다.

TimescaleDB 는 쓰지 않습니다. 하이퍼테이블을 하나도 만들지 않았고, RDS 에는
`pg_available_extensions` 에 아예 없어 설치할 수도 없습니다. 시세 보관은
하이퍼테이블이 아니라 PK 인덱스를 타는 기간 DELETE 로 처리합니다.

프로덕션에서는 `pg16` 대신 명시적인 PostgreSQL 버전으로 고정하세요.
