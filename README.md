# ETL Platform

Airflow로 실행되는 팀 공용 ETL 레포입니다. 이 레포는 데이터 수집, 변환, 적재 로직을 도메인별 패키지로 관리합니다.
ETL 실행, 스케줄, 재시도, 로그 확인은 Airflow가 담당합니다.

## Structure

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

## Conventions

- `dags/`에는 DAG 정의만 둡니다.
- 실제 ETL 로직은 `pipelines/{domain}/jobs/`에 둡니다.
- 외부 데이터 조회는 `extractors/`, 변환은 `transformers/`, 저장은 `loaders/`가 담당합니다.
- 여러 도메인에서 공유하는 코드는 `pipelines/common/`에 둡니다.
- Airflow task는 job 함수를 호출하고, job 함수가 extract-transform-load 흐름을 조립합니다.

## Local Checks

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

