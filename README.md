# ETL Platform

Airflow로 실행되는 팀 공용 ETL 레포입니다. 이 레포는 데이터 수집, 변환, 적재 로직을 도메인별 패키지로 관리합니다.

FastAPI는 포함하지 않습니다. ETL 실행, 스케줄, 재시도, 로그 확인은 Airflow가 담당합니다.

## Structure

```text
etl/
├── dags/                 # Airflow DAG 정의
├── pipelines/            # 도메인별 ETL 구현
│   ├── common/           # 설정, DB, 로깅, 재시도, 레이트리밋 공통 코드
│   ├── stocks/           # 국내 주식 OHLCV 수집
│   ├── disclosures/      # 공시 수집
│   └── news/             # 뉴스/이벤트 수집
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

## Stock Pipeline Policy

- 과거 일봉은 FinanceDataReader로 초기 적재합니다.
- 오늘 이후 장중 데이터는 한국투자증권 Open API의 1분봉 조회로 누적합니다.
- 5분봉은 API에서 직접 받지 않고 1분봉에서 내부 집계합니다.
- 1분봉은 기본 60일 보관, 5분봉은 3년 보관, 일봉은 영구 보관합니다.
- 사용자-facing 차트는 최대 5분 지연을 허용합니다.

## Local Checks

CI와 동일한 검사를 로컬에서 돌리려면 먼저 dev 의존성을 설치한다.

```bash
python -m pip install -e ".[dev]"
```

### 자동 (pre-commit)

한 번 설정하면 commit·push 때 검사가 자동으로 돈다.

```bash
pre-commit install                       # commit 시 ruff(자동수정) + ruff-format
pre-commit install --hook-type pre-push  # push 시 pytest
```

### 수동

```bash
ruff check . && ruff format --check .            # lint (CI: lint job)
pytest -m "not integration"                      # 유닛 테스트 (CI: unit-test job)
python -m compileall pipelines dags scripts
```

DAG 파싱 검증은 airflow가 필요하다(CI: dag-validation job).

```bash
python -m pip install -e ".[airflow]"
python scripts/validate_dags.py
```

DB 통합 테스트는 로컬 DB를 띄운 뒤 실행한다.

```bash
docker compose up -d db
pytest -m integration
```

