# Events Pipeline

`news_clusters`(같은 사건의 기사 묶음)를 Neo4j `Event` 노드로 올리고, 당사자 상장사와
`(Company)-[:HAS_EVENT]->(Event)` 로 잇는다. 용도는 기업 페이지 타임라인이다.

```cypher
MATCH (c:Company {ticker: $ticker})-[:HAS_EVENT]->(e:Event)
RETURN e ORDER BY e.last_published_at DESC LIMIT 20
```

## 흐름 (`jobs/promote_events.py`, news_pipeline 의 마지막 task)

1. RDB 에서 `original_size >= NEWS_EVENT_MIN_SIZE` 이고 최근 `NEWS_EVENT_SCAN_DAYS` 안에
   바뀐 클러스터를 읽는다.
2. Neo4j 에 이미 있는 Event 는 **매 런 전량 갱신** — 카운터·시간 범위·대표·`news_ids`·
   `keywords` 를 덮어쓰고 `companies` 로 간선을 다시 MERGE 한다. 제목은 불변.
3. 없는 클러스터는 **생성** — 멤버 기사(제목 + 요약 또는 리드)에서 gazetteer 로 후보 기업을
   뽑고, LLM 이 후보 안에서 당사자와 우산 제목을 고르고, 코드가 검증한 뒤 노드와 간선을 쓴다.
   후보 0 이면 LLM 을 부르지 않고 노드도 만들지 않는다. 런당 LLM 상한
   `NEWS_EVENT_MAX_ITEMS_PER_RUN`.

RDB 에는 아무것도 쓰지 않는다. 그래프가 유일한 저장소다.

## 배포 순서

1. `migrations/neo4j/0002_us_companies.cypher`(한글 `name` 키)와 `0003_events.cypher` 가
   neo4j-init 으로 적용돼 있을 것. neo4j-init 은 매 기동마다 전체를 재실행하며 둘 다 멱등이다.
2. `companies_sync_master.seed_graph` 가 한 번은 성공해 KRX `is_listed` 가 채워져 있을 것.
3. 그 뒤 `news_pipeline` 이 돌면 `promote_events` 가 붙는다.

## 1회성 백필

스캔 범위(15일)를 벗어난 Event 는 갱신되지 않으므로, 그 뒤에 시드된 기업의 간선은 아래로
한 번 붙인다.

```cypher
MATCH (e:Event)
UNWIND e.companies AS name
MATCH (c:Company {name: name}) WHERE c.is_listed = true
MERGE (c)-[:HAS_EVENT]->(e)
```

## 로컬 실행

```bash
uv run python scripts/run_job.py pipelines.events.jobs.promote_events:run
```

`.env` 에 `BEDROCK_*`, `AWS_BEARER_TOKEN_BEDROCK`, `NEO4J_*`, `DATABASE_URL` 이 있어야 한다.
비공개 파일(`pipelines/triples/ontology/`, `pipelines/events/prompts/`)이 로컬에 있어야 한다.
