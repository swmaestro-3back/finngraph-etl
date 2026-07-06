# Database Image

This image extends `timescale/timescaledb` and installs `pgvector`.

The database is still PostgreSQL. Extensions are enabled by init SQL:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
```

Use regular relational tables for stock master data, market calendars, ETL jobs, and metadata. Use TimescaleDB hypertables for OHLCV candles. Use pgvector tables for embeddings.

## Build Args

- `BASE_IMAGE`: defaults to `timescale/timescaledb:latest-pg16`
- `PGVECTOR_VERSION`: defaults to `v0.8.4`

For production, pin `BASE_IMAGE` to an explicit TimescaleDB and PostgreSQL version instead of `latest-pg16`.

