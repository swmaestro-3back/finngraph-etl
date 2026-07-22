# Database Image

로컬 DB는 공식 `timescale/timescaledb:latest-pg16` 이미지를 그대로 사용합니다.
별도 커스텀 빌드는 없습니다.

확장은 init SQL로 활성화됩니다:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

일반 관계형 테이블은 종목 master, 마켓 캘린더, ETL 작업 이력, metadata 등에 사용하고,
OHLCV 캔들 같은 시계열 데이터는 TimescaleDB hypertable로 다룹니다.

프로덕션에서는 `latest-pg16` 대신 명시적인 TimescaleDB/PostgreSQL 버전으로 고정하세요.
