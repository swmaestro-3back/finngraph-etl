# Themes Pipeline

주식 테마(테마주) 데이터를 여러 소스에서 크롤링해 Neo4j와 Postgres에 적재하는 파이프라인입니다.

## 개요

`naver`, `judal`, `antwinner` 3개 소스에서 테마명과 소속 종목을 크롤링(Extract) →
Neo4j에 이미 존재하는 테마/종목과 대조해 검증 및 병합(Validate) →
Neo4j(`Theme` 노드 + `BELONGS_TO` 관계)와 Postgres(`themes`/`theme_stocks`)에
각각 적재(Load)하는 구조입니다.

## 디렉터리 구조

```
pipelines/themes/
  models.py                      # Theme, Company pydantic 모델
  sources.py                     # 크롤링 대상 소스 목록 (의존성 0 — DAG가 파싱 시점에 읽음)
  extractors/                    # 읽기 (외부 소스)
    base.py, naver.py, judal.py, antwinner.py   # 소스별 크롤러
    factory.py                   # 소스명 → Extractor 생성
  references/                    # 읽기 (우리 저장소)
    graph.py                     # Neo4j 참조 조회 (검증용)
  transformers/                  # 변환
    validator.py                 # 검증/병합 로직
  loaders/                       # 쓰기 (저장소별 — 테이블별이 아님)
    neo4j.py                     # Theme 노드·BELONGS_TO 적재, 전량 삭제
    postgres.py                  # themes/theme_stocks 적재, news_themes 연결
  jobs/                          # Airflow task 진입점 (task 1개 = 파일 1개, 함수는 run)
  data/                          # 크롤링·검증 결과 JSON (날짜별 폴더, git 제외)
```

계층을 가르는 기준:

- **`extractors/`는 파이프라인의 입력**입니다. 외부 소스에서 새 데이터를 끌어옵니다.
- **`references/`는 우리 저장소를 읽기만 합니다.** 검증에 쓰는 그래프 조회가 여기 있습니다.
  `loaders/`에 두면 `transformers → loaders` 역방향 의존이 생기고, `extractors/`에 두면
  파이프라인 입력인 크롤러와 섞입니다 — 이 조회는 입력이 아니라 대조용 **참조**입니다.
  읽기/쓰기가 갈리는 기준이 아니라 **입력이냐 참조냐**가 `extractors`와 `references`를
  가릅니다.
- **`loaders/`는 저장소별로 나눕니다.** Neo4j 경로는 async + 쿼리 단위 자동 커밋,
  Postgres 경로는 sync + `session_scope` 트랜잭션이라 호출 규약과 롤백 범위가 다릅니다.
  파일이 경계면 그 차이를 파일 단위로 알 수 있습니다. 반대로 **테이블이나 호출하는 DAG가
  다르다는 이유로는 나누지 않습니다** — `postgres.py`가 themes·theme_stocks·news_themes를
  모두 쓰는 이유입니다. 호출 규약이 같으면 한 파일입니다.
- **`jobs/`는 Airflow와 도메인 코드 사이의 방화벽**입니다. `jobs/` 아래로는 Airflow가
  존재하지 않으므로 pytest·CLI에서도 그대로 돌릴 수 있습니다.

## DAG와 태스크

두 개의 DAG가 있고, **트리거가 달라서** 나뉘어 있습니다.

| DAG | 트리거 | 태스크 |
|---|---|---|
| `themes_refresh` | cron `0 0 * * *` | reset_graph → extract_{source} ×3 → validate_themes → (load_graph ∥ load_rdb) |
| `themes_link_news` | Asset `etl://news/relations` | link_news |

```
reset_graph ──▶ extract_naver ┐
            ├──▶ extract_judal ┼──▶ validate_themes ──┬──▶ load_graph (Neo4j)
            └──▶ extract_antwinner ┘                  └──▶ load_rdb   (Postgres)
                                                              │
                                                    Asset: etl://themes/rdb
```

태스크를 나눈 기준은 **재시도 경계**입니다 — "여기가 깨졌을 때 앞 단계를 다시 돌리고
싶은가?"에 아니라고 답하면 태스크를 나눕니다.

- **소스별 extract가 별개 태스크인 이유**: 외부 웹이 가장 잘 깨지는 지점이라 한 소스의
  실패가 나머지를 막으면 안 되고, 소스 간 독립이라 병렬 이득도 큽니다.
- **`validate_themes`가 단독인 이유**: 중복 테마 판정이 전체 소스를 함께 봐야 성립하는
  join 지점입니다. 여기가 깨져도 크롤링 결과는 파일에 남아 재사용됩니다.
- **`load_graph`와 `load_rdb`를 나눈 이유**: 저장소가 달라 실패 특성이 다릅니다. 한쪽이
  죽었다고 다른 쪽까지 되돌릴 이유가 없고, Neo4j 적재는 `reset_graph`부터 다시 도는
  구조라 재실행이 특히 비쌉니다.
- **`link_news`가 별개 DAG인 이유**: 태스크가 하나뿐이지만 트리거가 cron이 아니라
  뉴스 관계 추출 완료(Asset)입니다. 하나의 DAG는 스케줄을 하나만 가지므로 합칠 수 없습니다.

태스크 사이에는 JSON 파일 **경로만** XCom으로 넘깁니다. 테마 스냅샷이 수 MB라
XCom(메타DB)에 싣기엔 큽니다.

## 상세

- **Extract**: `extractors/{naver,judal,antwinner}.py` — 각 소스별 `BaseExtractor` 구현체.
  - `fetch_themes()`: 블랙리스트 제외한 테마명 크롤링
  - `extract_theme_stock()`: 테마별 소속 종목 크롤링
  - `run()` 결과는 `data/{YYYYMMDD}/{source}.json`으로 저장
  - `extractors/factory.py`의 `ExtractorFactory.get_extractor(source_name)`으로 소스명에
    맞는 구현체 생성. 소스를 추가할 때는 `sources.py`의 `SOURCES`와 이 팩토리를 함께
    고칩니다 (한쪽만 고치면 태스크는 생기고 실행 시점에 `ValueError`로 실패합니다).
- **Validate**: `transformers/validator.py`
  - `validate_company`: 크롤링한 종목명/ticker를 Neo4j 기존 `Company`와 대조, 불일치·미존재 종목 제외
  - `validate_theme`: 이름 동일 또는 이름 포함 관계 + 종목 overlap(임계값 0.9) 기준으로 기존/배치 내 테마와 병합
- **Load**: `loaders/neo4j.py`(그래프), `loaders/postgres.py`(RDB)

## 로컬 실행

개별 태스크는 job 모듈로 직접 돌릴 수 있습니다.

```bash
python scripts/run_job.py pipelines.themes.jobs.reset_graph:run
```

전체 흐름은 DAG 정의 그 자체를 실행하는 Airflow의 `dag.test()`로 확인합니다. 파이프라인
순서를 CLI용으로 다시 적어두면 DAG와 어긋나기 때문에 별도 오케스트레이션 스크립트는
두지 않습니다.

실행 전 `.env`에 `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`와
`DATABASE_URL`을 설정해야 합니다 (`pipelines/common/config.py` 참고).
