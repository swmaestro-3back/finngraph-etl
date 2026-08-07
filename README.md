# ETL Platform

Airflow로 실행되는 팀 공용 ETL 레포입니다. 이 레포는 데이터 수집, 변환, 적재 로직을 도메인별 패키지로 관리합니다.

## 구조

```text
etl/
├── dags/                 # Airflow DAG 정의
├── pipelines/            # 도메인별 ETL 구현
│   ├── common/           # 설정, DB(Postgres/Neo4j), 로깅, 재시도, 레이트리밋 공통 코드
│   ├── stocks/           # 국내 주식 OHLCV 수집
│   ├── disclosures/      # 공시 수집
│   ├── news/             # 뉴스/이벤트 수집
│   ├── themes/           # 테마-종목 매핑 수집
│   └── triplets/         # 뉴스 → 지식그래프 트리플 추출 (LangGraph 워크플로우)
├── migrations/           # DB migration
├── scripts/              # 로컬 실행/검증 스크립트
└── tests/                # 테스트
```

## 컨벤션

- `dags/`에는 DAG 정의만 둡니다.
- 실제 ETL 로직은 `pipelines/{domain}/jobs/`에 둡니다.
- 외부 데이터 조회는 `extractors/`, 변환은 `transformers/`, 저장은 `loaders/`가 담당합니다.
- 여러 도메인에서 공유하는 코드는 `pipelines/common/`에 둡니다.
- Airflow task는 job 함수를 호출하고, job 함수가 extract-transform-load 흐름을 조립합니다.

## 주식 파이프라인 정책

- 과거 일봉은 FinanceDataReader로 초기 적재합니다.
- 오늘 이후 장중 데이터는 한국투자증권 Open API의 1분봉 조회로 누적합니다.
- 5분봉은 API에서 직접 받지 않고 1분봉에서 내부 집계합니다.
- 1분봉은 기본 60일 보관, 5분봉은 3년 보관, 일봉은 영구 보관합니다.
- 사용자-facing 차트는 최대 5분 지연을 허용합니다.

## 로컬 기본 검사

CI와 동일한 검사를 로컬에서 돌리려면 먼저 의존성을 설치한다. `uv sync`는 dev
dependency-group(pytest/ruff/pre-commit)까지 기본으로 설치한다.

```bash
uv sync
```

### 자동 (pre-commit)

한 번 설정하면 commit·push 때 검사가 자동으로 돈다.

```bash
uv run pre-commit install                       # commit 시 ruff(자동수정) + ruff-format
uv run pre-commit install --hook-type pre-push  # push 시 pytest
```

### 수동

```bash
uv run ruff check . && uv run ruff format --check .   # lint (CI: lint job)
uv run pytest -m "not integration"                    # 유닛 테스트 (CI: unit-test job)
uv run python -m compileall pipelines dags scripts
```

DAG 파싱 검증은 airflow가 필요하다(CI: dag-validation job).

```bash
uv sync --extra airflow
uv run python scripts/validate_dags.py
```

DB 통합 테스트는 로컬 DB를 띄운 뒤 실행한다.

```bash
docker compose up -d db
uv run pytest -m integration
```

## Running Neo4j locally

```bash
# neo4j stand-alone setup
docker compose up -d neo4j

# 비밀번호 초기화 및 볼륨 초기화
docker compose down -v
```

neo4j:5 community 버전은 username은 무조건 neo4j여야함.

## Running Airflow locally

Airflow 3.3(LocalExecutor) 스택을 docker compose 프로파일로 띄운다. 프로젝트 코드는 Airflow와
같은 파이썬 환경에 설치된다(Airflow 3.x부터 SQLAlchemy 2.0을 써서 의존성 충돌이 없다).

```bash
cp .env.example .env                     # 값 채우기 (특히 news 변수, AIRFLOW_FERNET_KEY)
docker compose --profile airflow build   # 커스텀 이미지 빌드 (의존성 변경 시에만 재실행)
docker compose --profile airflow up -d
# UI: http://localhost:8080 (기본 계정 airflow/airflow)
docker compose --profile airflow down    # 중지. -v를 붙이면 메타DB/로그까지 초기화
```

- 기존 `docker compose up -d db` 워크플로는 그대로 동작한다(airflow 서비스는 profile로 분리).
- `dags/`, `pipelines/`, `scripts/`는 컨테이너에 마운트되므로 코드 수정이 즉시 반영된다.
  `pyproject.toml` 의존성이 바뀌면 이미지를 다시 빌드한다.
- 컨테이너 안에서 ETL DB 접속은 `db:5432`다(compose가 `DATABASE_URL`/`DB_HOST`/`DB_PORT`를 덮어씀).
- 타임존은 `Asia/Seoul` 고정 — 주식 장중 cron(`9-16 * * 1-5` 등)이 KST 기준으로 해석된다.
- DAG는 생성 시 일시정지 상태로 등록된다(`DAGS_ARE_PAUSED_AT_CREATION`). UI에서 unpause 후 사용한다.

## Database 초기화와 migration

로컬 DB는 `timescale/timescaledb` 기반이며 `pgvector` extension을 함께 사용한다. DB volume이 처음
만들어질 때 PostgreSQL이 `/docker-entrypoint-initdb.d`의 파일을 파일명 순서대로 자동 실행한다.

```text
docker/db/initdb/001_extensions.sql      # timescaledb + vector extension
docker/db/initdb/002_run_migrations.sh   # /migrations/versions/*.sql 파일명 순 실행
migrations/versions/*.sql                # migration 파일 (컨테이너에 마운트됨)
```

자동 초기화는 volume이 처음 생성될 때만 돈다. **이미 `etl_pgdata` volume이 있으면 새 migration을
수동으로 적용해야 한다.**

```bash
docker compose exec -T db psql -U etl -d etl < migrations/versions/<파일명>.sql
```

## 로컬에서 Job 실행

Airflow 없이 job 함수를 직접 실행할 수 있다.

```bash
python scripts/run_job.py pipelines.stocks.jobs.sync_stock_master:run
```

위 명령은 KIS public master 파일에서 KOSPI/KOSDAQ 종목 master 데이터를 가져와 `stocks` 테이블에
동기화한다. DB가 실행 중이고 stocks migration이 적용되어 있어야 한다. 종목 master 동기화는 공개
파일을 내려받으므로 `KIS_APP_KEY` 등의 인증 정보는 필요 없다(장중 API job에만 필요).
