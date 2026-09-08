# Database Image

로컬 DB는 공식 `pgvector/pgvector:pg16` 이미지를 그대로 사용합니다.
별도 커스텀 빌드는 없습니다.

확장은 init SQL로 활성화됩니다:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

`vector` 는 테마 임베딩(`themes.embedding`, `theme_stocks.reason_embedding`) 같은
pgvector 컬럼에 필요합니다. 운영 DB(RDS)도 같은 확장을 씁니다.

TimescaleDB 는 쓰지 않습니다. 하이퍼테이블을 하나도 만들지 않았고, RDS 에는
`pg_available_extensions` 에 아예 없어 설치할 수도 없습니다. 시세 보관은
하이퍼테이블이 아니라 PK 인덱스를 타는 기간 DELETE 로 처리합니다.

프로덕션에서는 `pg16` 대신 명시적인 PostgreSQL 버전으로 고정하세요.
