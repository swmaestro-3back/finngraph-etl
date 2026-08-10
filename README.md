# finngraph-etl

뉴스·기업·주가·테마·삼중항관계 데이터를 수집 > 가공 > 적재하는 Airflow 기반 ETL 파이프라인.

## Structure

```text
finngraph-etl/
├── dags/                 # Airflow DAG 정의 (도메인별 디렉토리)
│   ├── health/           # 운영 헬스체크
│   ├── news/             # 뉴스 수집 · 필터 · 요약
│   ├── stocks/           # 주가 캔들 수집 · 집계
│   ├── themes/           # 테마 크롤링
│   └── triplets/         # 삼중항(관계) 추출
├── pipelines/            # 도메인별 ETL 구현
│   ├── common/           # ETL 내 사용되는 공통 모듈
│   ├── stocks/           # 주식 및 주가 ETL
│   ├── news/             # 뉴스 ETL
│   ├── themes/           # 테마 ETL
│   └── triplets/         # 삼중항관계 ETL
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

## Running Airflow locally

Airflow 3.3(LocalExecutor) 스택을 docker compose 프로파일로 띄운다. 프로젝트 코드는 Airflow와
같은 파이썬 환경에 설치된다(Airflow 3.x부터 SQLAlchemy 2.0을 써서 의존성 충돌이 없다).

```bash
# 환경변수 설정
cp .env.example .env

# 최초 1회 (또는 의존성/Dockerfile 변경된 경우)
# 이미지 빌드
docker compose --profile airflow build

# airflow 관련 스택 기동
docker compose --profile airflow up -d  

# Airflow 컨테이너 중지
docker compose --profile airflow down

# Airflow 컨테이너 중지 및 볼륨(메타DB/로그)까지 삭제
dockre compose --profice airflow down -v
```

- Airflow Web UI는 `http://localhost:8080`로 접속한다.
  - 기본 계정은 airflow/airflow이다.
- `dags/`, `pipelines/`, `scripts/`는 컨테이너에 마운트되므로 코드 수정이 즉시 반영된다.
  `pyproject.toml` 의존성이 바뀌면 `airflow build`로 이미지를 다시 빌드해야 한다.
- 컨테이너 안에서 ETL DB 접속은 `db:5432`다(compose가 `DATABASE_URL`/`DB_HOST`/`DB_PORT`를 덮어씀).
- 타임존은 `Asia/Seoul` 고정 — 주식 장중 cron(`9-16 * * 1-5` 등)이 KST 기준으로 해석된다.
- DAG는 생성 시 일시정지 상태로 등록된다(`DAGS_ARE_PAUSED_AT_CREATION`). UI에서 unpause 후 사용한다.

### 스택 구성

`--profile airflow`는 Airflow 컨테이너 하나가 아니라 역할별로 분리된 여러 서비스를 함께 띄운다
(Airflow 3.x 방식). `dags/`의 파이썬 파일은 "무엇을 언제 어떤 순서로 실행할지"를 **정의만** 하고,
실제 실행은 아래 컨테이너들이 담당한다.

| 서비스 | 역할 |
| --- | --- |
| `airflow-init` | 최초 1회 DB 마이그레이션 + admin 계정 생성 후 종료 |
| `airflow-apiserver` | 웹 UI + Execution API (포트 8080) |
| `airflow-scheduler` | 스케줄 판단 + LocalExecutor라 태스크 실제 실행도 여기서 수행 |
| `airflow-dag-processor` | `dags/`를 파싱해 DAG 등록 (Airflow 3부터 scheduler에서 분리) |
| `airflow-db` | Airflow 메타데이터 전용 Postgres (ETL 데이터 `db`와 분리) |

- `apache-airflow`는 `pyproject.toml`의 optional-dependency라 기본 `uv sync`엔 설치되지 않는다.
  DAG 파일이 `from airflow.sdk import ...`를 `try/except`로 감싸는 것은, airflow가 없는 로컬 편집·CI
  문법검사 환경에서도 파싱이 깨지지 않게 하기 위함이다. 실제 실행은 airflow가 설치된 컨테이너 안에서만 된다.
- 데코레이터로 정의한 DAG 함수는 파일 최상단에서 한 번 호출해야 dag-processor가 인식·등록한다.

## Running Neo4j locally

```bash
# Neo4j 컨테이너만 실행
docker compose up -d neo4j

# Neo4j 컨테이너 중지

# Neo4j 컨테이너 중지 및 볼륨까지 삭제
docker compose down -v
```

- neo4j:5 community 버전은 username은 무조건 neo4j여야한다.
- 기본 계정 정보는 처음 이미지 빌드 시 `.env` 정보로 인해 초기화된다.
  - 비밀번호가 바뀐 경우 `docker compose down -v` 옵션으로 볼륨을 내리고 다시 실행시켜야 한다.

## Local Checks Before Commit

```bash
uv sync
```
- CI와 동일한 검사를 로컬에서 돌리기 위해 먼저 관련 의존성 패키지(pytest/ruff/pre-commit)를 설치해야 한다.
- CI 관련 패키지는 `dev dependency-groups`로 지정되어 
`uv sync`시 필수 패키지와 같이 설치된다.

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
