# Events Pipeline

`news_clusters`(같은 사건의 기사 묶음)를 Neo4j `Event` 노드로 올리고, 당사자 상장사와
`(Company)-[:HAS_EVENT]->(Event)` 로 잇습니다. 용도는 기업 페이지 타임라인입니다.

```cypher
MATCH (c:Company {ticker: $ticker})-[:HAS_EVENT]->(e:Event)
RETURN e ORDER BY e.first_published_at DESC LIMIT 20
```

## 기동 (`dags/news/cluster_articles.py`, `news_cluster_articles` 의 `generate_events` task)

별도 DAG 가 아니라 클러스터 DAG 의 task 입니다. `promote_clusters` 뒤에 `summarize_articles` 와
나란히 돕니다. `promote_clusters` 가 Asset 발행을 건너뛰어도(삼중항 미처리 대표 0건) 이 task 는
돕니다(`trigger_rule="none_failed"`).

```
news_collect_articles ──► etl://news/articles ──► news_cluster_articles
                                                   assign_clusters → promote_clusters → generate_events
```

## 흐름

task 하나(`generate_events`, `jobs/generate_events.py`)가 돕니다. LLM 을 부르지 않습니다.

1. **후보** — `news_clusters` 에서 대표와 제목이 있고 후보가 `NEWS_CLUSTER_PROMOTE_SIZE` 건 이상이며
   최근 `NEWS_EVENT_SCAN_DAYS` 안에 갱신된 클러스터를 읽고, Neo4j 에 `Event {cluster_id}` 가 있는지로
   나눕니다(`scan.py` 의 `scan_events`). 노드가 있는 클러스터는 `last_published_at` 만 갱신하고(아래),
   없는 클러스터가 2·3 단계로 갑니다.
2. **당사자** — 클러스터 후보 기사 중 `NEWS_EVENT_COMPANY_MIN_ARTICLES`(기본 2)건 이상의
   `news_companies` 에 든 기업입니다. 사건의 당사자는 후보 대부분에 나오고 지나가는 언급은 한두 건에만
   나오므로, 한 기사의 오판정이 결과를 바꾸지 못합니다. 승격 후 기사의 연결은 세지 않습니다. 당사자가
   0개면 노드를 만들지 않고(`skipped_no_companies`) 다음 런에 다시 봅니다.
3. **쓰기** — `Event` 노드를 만들고 `Company.company_id`(Postgres `companies.id` 미러)로 `is_listed` 인
   노드를 찾아 `HAS_EVENT` 로 잇습니다. 클러스터 하나가 실패 단위이고, 실패한 클러스터는 노드가 없으니
   다음 런에 다시 시도됩니다.

기업 추출과 판정은 뉴스 수집(`collect_articles`)이 끝내 `news_companies` 에 저장했습니다 — 제목의
기업은 관련성 필터가, 본문에만 나온 기업은 엔티티 필터가 판정합니다.

노드에는 `cluster_id`, `title`, `first_published_at`, `last_published_at`, `created_at` 만 있습니다.
키워드·기사 목록·기사 수는 `cluster_id` 로 RDB 에서 읽습니다. 만든 뒤 바뀌는 값은 `last_published_at`
하나입니다. 승격 이후에 기사가 붙으면 `news_clusters.last_published_at` 과 `updated_at` 이 갱신되고,
같은 런의 이 task 가 노드의 값을 커지는 방향으로만 올립니다(`update_last_published`). 갱신이 실패해도
생성은 계속 돌고, 스캔 범위 안이면 다음 런이 다시 올립니다.

기간 [start, end] 에 걸친 사건은 두 시각의 구간 겹침으로 찾습니다. 두 속성 모두 인덱스가 있습니다.

```cypher
MATCH (e:Event)
WHERE e.first_published_at <= $end AND e.last_published_at >= $start
RETURN e
```

제목은 `news_clusters.title`, 당사자는 `news_companies` 에 있으므로 그래프에서 Event 가 사라져도 스캔
범위 안이면 다음 런이 RDB 만으로 다시 만듭니다. 생성 시점에 `Company` 노드가 없던 기업은 간선이
빠지는데, 노드를 지우고 다시 생성하면 붙습니다.

DAG 런끼리는 `max_active_runs=1` 로 직렬화합니다. RDB 에는 아무것도 쓰지 않습니다.

## 배포 순서

1. `0003_events.cypher` 가 neo4j-init 으로 적용돼 있을 것. neo4j-init 은 매 기동마다
   전체를 재실행하며 멱등입니다.
2. `companies_sync_master.seed_graph` 가 한 번은 성공해 KRX `is_listed` 가 채워져 있을 것.
3. US Company 노드는 `companies_load_us` DAG 의 `load_neo4j` task(`jobs/load_us_neo4j.py`)가
   동적으로 시드합니다 — 마이그레이션에 들어 있지 않으므로 `docker compose down -v` 뒤에는
   `companies_crawl_us` 를 한 번 수동 실행해야 합니다(뒤이어 `companies_load_us` 가 Asset 으로
   자동으로 따라붙어 US 기업 간선이 붙습니다).
4. `dags/news/collect_articles.py`(outlet)와 `dags/news/cluster_articles.py`(구독) 사이에 배포 순서 제약은
   없습니다. 구독 DAG 만 있으면 Asset 이 발행될 때까지 기다리고, outlet 만 있으면 소비자
   없는 Asset 이벤트가 기록될 뿐입니다.

## 로컬 실행

```bash
uv run python scripts/run_job.py pipelines.events.jobs.generate_events:run
```

`.env` 에 `NEO4J_*`, `DATABASE_URL` 이 있어야 합니다.
