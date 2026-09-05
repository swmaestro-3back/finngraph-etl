# Events Pipeline

`news_clusters`(같은 사건의 기사 묶음)를 Neo4j `Event` 노드로 올리고, 당사자 상장사와
`(Company)-[:HAS_EVENT]->(Event)` 로 잇습니다. 용도는 기업 페이지 타임라인입니다.

```cypher
MATCH (c:Company {ticker: $ticker})-[:HAS_EVENT]->(e:Event)
RETURN e ORDER BY e.last_published_at DESC LIMIT 20
```

## 기동 (`dags/events/pipeline.py`, `events_pipeline`)

시각이 아니라 Asset 을 구독합니다. `news_pipeline.collect_articles` 가 클러스터를 하나라도
생성·갱신한 런에서만 `etl://news/clusters` 를 발행하고, 그 신호로 이 DAG 이 깨어납니다.
클러스터 변경이 0건인 시간에는 `collect_articles` 가 skip 돼 발행이 없고, 이 DAG 도 돌지
않습니다.

```
news_pipeline.collect_articles ──► etl://news/clusters ──► events_pipeline
```

## 흐름

두 task 가 병렬로 돕니다. 둘 다 `references/scan.py` 의 `scan_promotable` 로 후보 클러스터
(`original_size >= NEWS_EVENT_MIN_SIZE`, 최근 `NEWS_EVENT_SCAN_DAYS` 안에 변경)를 읽고
Neo4j 존재 여부로 **자기 몫만** 고르므로, task 사이에 XCom 이 없습니다. 각 task 의
`scanned` 는 후보 전체가 아니라 자기 몫의 수입니다.

- **`sync_events`** (`jobs/sync_events.py`) — 이미 Event 가 있는 클러스터를 **매 런 전량
  갱신**합니다. 카운터·시간 범위·대표·`news_ids`·`keywords` 를 RDB 값으로 덮어쓰고
  `companies` 로 간선을 다시 MERGE 합니다. 제목·`companies` 는 불변이고 LLM 을 부르지
  않습니다. 배치 하나라 실패하면 통째로 `refresh_failed` 로 셉니다.
- **`generate_events`** (`jobs/generate_events.py`) — Event 가 없는 클러스터를 **생성**합니다.
  멤버 기사(제목 + 요약 또는 리드)에서 gazetteer 로 후보 기업을 뽑고, LLM 이 후보 안에서
  당사자와 우산 제목을 고르고, 코드가 검증한 뒤 노드와 간선을 씁니다. 후보 0 이면 LLM 을
  부르지 않고 노드도 만들지 않습니다. 런당 LLM 상한 `NEWS_EVENT_MAX_ITEMS_PER_RUN`. 클러스터
  하나가 실패 단위이고, 실패한 클러스터는 노드가 없으니 다음 런에 다시 시도됩니다. 처리할
  클러스터가 없으면 gazetteer·Bedrock 클라이언트를 만들지 않습니다.

두 task 가 겹쳐도 데이터는 깨지지 않습니다. 둘 다 `cluster_id` 로 MERGE 하고, `sync_events`
는 `generate_events` 가 쓰는 `title`·`companies` 를 건드리지 않습니다. 같은 클러스터에 LLM
을 두 번 부르지 않도록 DAG 런끼리는 `max_active_runs=1` 로 직렬화합니다.

RDB 에는 아무것도 쓰지 않습니다. 그래프가 유일한 저장소입니다.

Neo4j 를 유일한 저장소로 둔 데는 트레이드오프가 있습니다. 그래프에서 Event 가 사라지면
(오작동, 수동 삭제 등) LLM 재실행 말고는 복구 경로가 없습니다 — RDB 에 별도 원장이 없기
때문입니다. 또한 생성 시점에 해석되지 않은 당사자(미시드·비상장 기업)는 노드의
`companies` 배열에 이름만 남고 간선은 생기지 않는데, 그 기업이 나중에 시드되면 다음 갱신이
간선을 붙입니다 — 아래 "1회성 백필" 은 그 간극을 스캔 범위 밖 Event 에 대해 메웁니다.

## 배포 순서

1. `migrations/neo4j/0002_us_companies.cypher`(한글 `name` 키)와 `0003_events.cypher` 가
   neo4j-init 으로 적용돼 있을 것. neo4j-init 은 매 기동마다 전체를 재실행하며 둘 다 멱등입니다.
   구 0002(영문 `name`)가 적용된 로컬 Neo4j 볼륨에서는 `[1]` 리네임 단계가
   `company_name_unique` 위반으로 실패할 수 있으므로 `docker compose down -v` 후
   재기동합니다(dev 에는 구 0002 가 적용된 적이 없습니다).
2. `companies_sync_master.seed_graph` 가 한 번은 성공해 KRX `is_listed` 가 채워져 있을 것.
3. `dags/news/pipeline.py`(outlet)와 `dags/events/pipeline.py`(구독) 사이에 배포 순서 제약은
   없습니다. 구독 DAG 만 있으면 Asset 이 발행될 때까지 기다리고, outlet 만 있으면 소비자
   없는 Asset 이벤트가 기록될 뿐입니다.

## 1회성 백필

스캔 범위(15일)를 벗어난 Event 는 갱신되지 않으므로, 그 뒤에 시드된 기업의 간선은 아래로
한 번 붙입니다.

```cypher
MATCH (e:Event)
UNWIND e.companies AS name
MATCH (c:Company {name: name}) WHERE c.is_listed = true
MERGE (c)-[:HAS_EVENT]->(e)
```

## 로컬 실행

```bash
uv run python scripts/run_job.py pipelines.events.jobs.sync_events:run
uv run python scripts/run_job.py pipelines.events.jobs.generate_events:run
```

`.env` 에 `BEDROCK_*`, `AWS_BEARER_TOKEN_BEDROCK`, `NEO4J_*`, `DATABASE_URL` 이 있어야 합니다.
비공개 파일(`pipelines/triples/ontology/`, `pipelines/events/prompts/`)이 로컬에 있어야 합니다.
