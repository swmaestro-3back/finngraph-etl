# News Pipeline

뉴스 수집·클러스터링·요약 파이프라인입니다. 관련성 필터를 통과한 기사는 본문과 연결 기업(`news_companies`)까지
채워 저장하고, 본문을 가져오지 못한 기사는 저장하지 않습니다.

## 수집 원천

`jobs/collect_articles.run(theme_ids)` 가 받은 테마의 편입 종목을 기업 단위로 모으고
(`repositories/postgres/search_history.py`), 기업마다 `NEWS_SEARCH_QUERY_TEMPLATES`(기본 `{name},공급`·`{name},계약`·`{name},수혜`·`{name},호재`·`{name},악재`·`{name},특징주` 여섯 개)로
검색어를 만들어 서식마다 네이버 뉴스 검색 API 를 최신순으로 호출합니다
(`extractors/search_collector.py`). 같은 기업이 여러 테마·여러 종목으로 나오면 한 번만
검색하고, 종목명은 stock id 순 첫 종목의 것을 씁니다.

기업별 마지막 검색 시각은 `search_history` 에 있습니다. `last_searched_at` 이
`NEWS_SEARCH_INTERVAL_HOURS` 를 넘긴 기업만 이번 런의 대상이고, 그 값이 워터마크가 되어
그보다 오래된 기사가 나오면 페이지를 멈춥니다. 기록이 없는 첫 검색은
`NEWS_SEARCH_LOOKBACK_DAYS` 까지만 거슬러 갑니다. 종목당 `NEWS_SEARCH_MAX_PAGES` 가
상한입니다. 런이 끝나면 검색에 성공한 기업만 런 시작 시각으로 갱신합니다 — 실패한 기업은
이전 워터마크를 유지해 다음 런에 같은 창을 다시 읽습니다.

`search_keywords` 테이블은 예전 키워드 검색의 원천으로 보존돼 있지만 현재 파이프라인은
참조하지 않습니다.

## 테마 선정 (jobs/select_themes.py)

런마다 백엔드가 Redis 에 발행한 핫테마(`etl:hot-themes`, `HOT_THEMES_REDIS_URL`)를 읽어 그
테마를 그대로 씁니다(`repositories/redis/hot_themes.py`). 선정과 발행은 백엔드 몫이고, ETL 은
`stocks_intraday_candles`(장중 매시)·`stocks_compute_derived`(마감 후)의 `publish_hot_themes` 로 백엔드
내부 API(`BACKEND_INTERNAL_URL`·`INTERNAL_API_TOKEN`)를 호출해 발행 시점만 보장합니다.

폴백은 없습니다. 아래 경우 태스크가 실패합니다:

- Redis 조회 실패, 키 없음, 페이로드 파싱 실패
- 페이로드 `tradeDate` 가 null 이거나 DB 거래일 범위 밖 — 밸류에이션까지 있는 마감일(`settled`)부터
  일봉만 있는 최신일(`latest`)까지가 허용 범위입니다(`stocks/repositories/postgres/stock_candles.py` 의
  `fetch_trade_dates`). 장중 발행분은 당일 일봉만 있어도 통과합니다.

수동 트리거 conf 로 `theme_ids` 를 주면 Redis 조회 없이 그 테마만 검색합니다:

```json
{"theme_ids": [12, 34]}
```

처음 검색하는 기업은 lookback 전체를 읽고 새 기사를 전부 LLM 에 보내므로, 테마는 한 번에
몇 개씩만 넘기세요.

## 단계 (jobs/collect_articles.py)

수집 → 제목 기업 매치(제목 × 기업 개체 사전, `transformers/company_matches.py`: 제목에 검색 종목이
없는 기사 제거, 제목의 상장사는 판정 대상으로) → 배치 URL 중복 제거(먼저 걸린 종목만 남김 — 매치가
먼저라 남는 사본은 검색 종목이 제목에 있는 것뿐) → 제목 필터(제외 패턴·종목 나열 제목 탈락 + 통과 기사 제목 선두 브라켓 제거, `transformers/filters/title_filter.py`) → DB 저장된 URL 제거(메모리 필터를 다 거친 뒤 DB 조회 1회) →
LLM 관련성 필터(제목만, 판정 기업마다, `transformers/filters/relevance_filter.py`) →
본문 크롤링 → 본문 기업 매치 → LLM 엔티티 필터(본문, `transformers/filters/entity_filter.py`) →
본문과 함께 저장 → `news_companies` 연결 → `search_history` 갱신.

- 관련성 필터가 본문 크롤링 앞에 있어 무관·시황 기사는 크롤링하지 않습니다. 저장되지 않으므로
  클러스터 프로필도 오염시키지 않습니다.
- 기사 하나는 검색 대상 종목 하나에 속합니다(`_query_company`). **제목**에 검색 종목이 없으면
  버립니다. 언급은 문자열이 아니라 기업으로 확인합니다 — 개체 사전 매치 중 같은 `company_id` 가
  있어야 해서 약칭('LG엔솔')으로만 적힌 제목도 통과하고, 다른 상장사 이름의 일부('SK하이닉스' 안의
  'SK')는 언급으로 치지 않습니다. 매처는 대소문자를 구분합니다.
- LLM 은 제목만 봅니다(스니펫은 검색어 주변 발췌라 넘기지 않습니다). 제목에 나온 상장사(`판정 기업`,
  검색 종목 첫 번째)마다 `is_company`(제목의 그 표기가 그 기업을 가리키는지 — '폴로하이브머티리얼즈'
  안의 '하이브', '임직원 대상'의 '대상'은 false)와 `valid`(GATE 1~3)를 내고, 둘 다 true 여야 그 기업이
  통과입니다. 저장 여부는 이 기사를 가져온 검색 종목 중 하나라도 통과했는지로 정합니다 — 같은 기사가
  여러 종목 검색에 걸리면 URL 중복 제거가 사본 하나로 합치며 걸린 검색 종목을 모두 기억합니다
  (`_query_companies`). 검색하지 않은 제목 기업만 통과하면 저장하지 않습니다. 호출은 `NEWS_LLM_BATCH_SIZE`(기본 10)건씩
  묶고, 응답은 기사 번호로 짝을 맞춥니다. 묶음이 실패하거나 번호·검색 종목 판정이 빠지면 그 기사만
  개별로 한 번 더 판정합니다.
- 본문 크롤링은 관련성을 통과한 기사에만 돕니다. 본문을 못 가져온 기사는 저장하지 않습니다.
  그래서 `news` 의 모든 행에 본문이 있습니다(`save_news`).
- 기업 연결은 두 단계입니다. 제목에 나온 기업은 관련성 필터의 판정으로 확정합니다. 본문에만 나온
  기업은 개체 사전으로 찾아(`match_body_companies`) 엔티티 필터가 "그 표기가 그 기업을 가리키고,
  기사가 서술하는 구체적 사업 행위·사실의 당사자인가"를 판정합니다. 애매하면 버리고, LLM 이 판정을
  빠뜨린 표기도, 인용한 문장(`mention`)에 표기가 없는 유지 판정도 버립니다. 제목에서 판정받은 기업은
  본문으로 다시 판정하지 않습니다. 본문에 추가 기업이 없는 기사는 엔티티 필터를 부르지 않습니다.
- 여러 종목을 모은 기사(공시·시황 모음, 다이제스트)는 저장하지 않습니다. 문단마다 다른 기업의 사건이
  실려 있어 제목과 무관한 기업이 연결되기 때문입니다. 두 곳에서 거릅니다. 제목 필터는 종목이
  구분자(가운뎃점·쉼표·빗금)로 4개 이상 이어진 제목과, 제목 전체가 나열뿐인 제목("A·B·C 등")을
  버립니다. 본문 기업 매치 뒤에는 본문 후보 표기가 `NEWS_BODY_CANDIDATE_MAX`(기본 15)를 넘는 기사를
  엔티티 필터 없이 버립니다.
- `news_companies` 는 수집 단계만 씁니다(`repositories/postgres/news_companies.py`). 기사 저장과 같은
  트랜잭션에서(`save_news`) 제목 통과 기업과 본문 통과 기업 전부에 연결하므로, 저장되는 기사는 통과한 검색 종목 하나 이상에 반드시 연결됩니다.
  클러스터·Event·삼중항 단계는 이 연결을 읽기만 합니다.
- 엔티티 필터 호출이 한 기사에서 실패하면 그 기사는 제목 기업만 연결해 저장합니다. 호출한 기사가
  전부 실패하면 저장 전에 task 를 실패시켜 재시도합니다(관련성 필터도 같습니다).
- 버린 기사는 따로 기록하지 않습니다. 워터마크가 다음 런의 창 밖으로 밀어냅니다. 관련성에서
  떨어졌거나 본문을 못 가져온 기사는 행이 없으므로, 다른 종목 검색으로 다시 들어오면 한 번 더
  판정합니다.
- 수집 잡은 클러스터를 판정하지 않습니다. 아래 "클러스터와 하류" 절을 봅니다.

## 클러스터와 하류 (jobs/cluster_articles.py)

수집 DAG 는 새 기사를 저장하면 `etl://news/articles` 를 발행하고, `news_cluster_articles` DAG 가
이어서 돕니다: `assign_clusters` → `promote_clusters` → `generate_events` ∥ `summarize_articles`,
요약 뒤에 `link_issues`(아래 "이슈 타임라인" 절).

- **판정** (`assign`, `transformers/clustering/online.py`): `cluster_id` 가 없는 저장 기사를 발행
  시각순으로 하나씩, 시간 창(클러스터 첫 기사 1일 전 ~ 7일 뒤) 안 클러스터의 프로필과 비교합니다.
  문서는 제목 + 본문 앞 `NEWS_CLUSTER_LEAD_CHARS` 자입니다. 유사도가 `NEWS_CLUSTER_THRESHOLD` 이상이면
  가장 가까운 클러스터에 편입하고, 아니면 새 클러스터를 만듭니다. 기록에 실패한 기사는 다음 런에
  다시 판정됩니다.
- **후보와 대표**: 클러스터는 후보를 `NEWS_CLUSTER_PROMOTE_SIZE`(기본 3)건까지만 받습니다. 후보가 다
  차면 저장된 본문으로 대표 1건을 고릅니다(`transformers/clustering/representative.py`). 크롤링하지
  않습니다. 그 뒤에 붙는 기사는 `cluster_id` 만 받고 `original_size` 를 올립니다.
- **제목**: 후보 기사 전부의 제목을 LLM 에 넣어 사건 이름을 짓습니다(`transformers/cluster_titler.py`,
  프롬프트 `transformers/prompts/cluster_title.py`). `NEWS_CLUSTER_TITLE_MAX_CHARS`(기본 25)를 넘으면
  축약 지시를 붙여 한 번 더 묻고, 그래도 실패한 클러스터는 이름이 NULL 로 남아 다음 런에 다시
  시도됩니다.
- **하류**: Event 생성·요약·삼중항 추출은 대표 기사가 정해진 클러스터만 합니다. 삼중항은
  `promote_clusters` 가 발행하는 `etl://news/clusters` 를 따라 `triples_extract_triples` 가 처리하고,
  엔티티는 `news_companies` 에서 읽습니다. 후보가 기준에 못 미친 사건은 추출하지 않습니다.
- `promote_clusters` 는 삼중항 미처리 대표 기사가 남아 있을 때만 Asset 을 발행합니다. 추출에 실패한
  대표는 미처리로 남아 다음 클러스터 런(다음에 새 기사가 저장된 시점)에 다시 깨워집니다.
- 수집 잡은 클러스터를 쓰지 않고 이 DAG 가 `max_active_runs=1` 로 직렬화하므로, 수집이 겹쳐도 같은
  사건에 클러스터가 둘 생기지 않습니다. 백필은 클러스터 DAG 를 따로 둡니다(아래 "KRX100 백필" 절).

설계: `docs/superpowers/specs/2026-10-04-news-pipeline-three-dags-design.md`,
`transformers/clustering/docs/` 의 세 문서.

## 이슈 타임라인 (jobs/link_issues.py)

이슈 하나(`news_clusters` 한 행 — 백엔드 `/api/v1/issues`)의 타임라인은 같은 이야기를 다룬 앞선
이슈의 사슬입니다(실적 프리뷰 → 실적 발표, 7월 건설지출 → 8월 건설지출). `link_issues` 가 이슈마다
부모 하나를 골라 `parent_cluster_id`·`story_root_id`(이야기 첫 이슈, 루트는 자기 id)·`link_score`
(부모와의 코사인)·`link_relation`·`linked_at` 을 씁니다. 백엔드는 요청 이슈에서 `parent_cluster_id`
를 따라 올라간 사슬을 최신순으로 보여 주고(후속으로 갈라진 다른 갈래는 넣지 않습니다),
`same_event` 로 이어진 이슈는 한 노드로 접습니다. LLM 은 부르지 않습니다 — 노드 제목은
`news_clusters.title`, 한 줄 요약은 대표 기사 핵심 포인트의 `CHANGE` 항목(없으면 `news.summary` 첫
문장)입니다. 판정 규칙은 `transformers/issue_linker.py`, 조회·쓰기는
`repositories/postgres/news_clusters.py` 의 "이슈 타임라인" 절에 있습니다.

- **대상**: 이름과 대표가 있고 `linked_at` 이 NULL 인 클러스터를 첫 기사 시각순으로, 지금부터
  `NEWS_ISSUE_LINK_LOOKBACK_DAYS` 안에서 런당 `NEWS_ISSUE_LINK_MAX_PER_RUN` 개. 판정마다 커밋하므로
  같은 런의 앞선 대상이 뒤 대상의 부모 후보가 됩니다.
- **요약 대기**: 대표 기사에 요약(포인트나 문단)이 아직 없으면 클러스터 `updated_at` 이 24시간 넘게
  지난 뒤에만 요약 없이 잇습니다. `updated_at` 은 승격·제목 생성 때 갱신되므로 그동안 클러스터 DAG
  런마다 요약이 다시 시도됩니다. 후속 기사가 붙을 때도 갱신되어, 기사가 계속 붙는 이슈는 그만큼
  더 기다립니다(대개 클러스터 시간 창 `NEWS_CLUSTER_WINDOW_DAYS` 안에서 끝납니다).
- **임베딩**: 제목, 한 줄 요약, 멤버 기사 제목 3개(후보 기사 먼저 발행 시각순)를 줄바꿈으로 이어
  Titan v2(1024)로 `news_clusters.embedding` 에 저장합니다. 후보 기사는 승격 전에 다 차고 바뀌지
  않으므로 승격 직후에 만들든 클러스터가 자란 뒤(백필·평가 스크립트)에 만들든 입력이 같습니다.
  모델은 테마용 `BEDROCK_EMBEDDING_MODEL` 과 따로 `NEWS_ISSUE_EMBEDDING_MODEL` 입니다. 한 번 저장한
  임베딩은 다시 만들지 않습니다.
- **주요 기업**: 이슈의 `news_companies` 기업 중 멤버 기사 제목 하나 이상에 나온 기업입니다. 제목
  매치는 수집 잡과 같은 개체 사전 매처(`common/gazetteer.py`)라 약칭도 같은 기업으로 잡고, 본문에만
  스친 거래처·경쟁사는 빠집니다. 제목에 아무 기업도 안 걸리면 전체 기업으로 돌아갑니다.
- **부모**: 대상보다 먼저 시작했고(첫 기사 시각, 같으면 id) 대상 시작 전 lookback 안이며 이미
  판정된 이슈 중, 주요 기업이 하나 이상 겹치고 코사인이 `NEWS_ISSUE_LINK_THRESHOLD` 이상인 것.
  기업이 없는 이슈는 기업이 없는 이슈에만 `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD` 이상으로 잇습니다.
  코사인 최대, 같으면 늦게 시작한 쪽, 그래도 같으면 큰 id 가 부모이고, 없으면 루트입니다. 후보는
  코사인 순으로 30개씩 읽고, 주요 기업 규칙을 넘는 후보가 나올 때까지 다음 페이지를 읽습니다(SQL 은
  본문에만 스친 기업까지 함께 걸러, 그런 후보가 위를 채우면 맞는 부모가 아래에 있을 수 있습니다).
- **꼬리 재판정**: 이슈는 시작 순서가 아니라 승격·요약을 받는 순서로 들어옵니다. 먼저 시작한 이슈가
  늦게 들어오면(후보가 늦게 차거나 요약이 늦을 때) 그 뒤에 시작해 이미 판정된 이슈는 그 이슈를
  부모로 못 본 채 판정돼 있습니다 — 같은 사건이 갈린 두 클러스터가 서로 루트로 남는 식입니다. 그래서
  런마다 이번 대상 중 가장 먼저 시작한 것보다 뒤에 시작한 판정(꼬리)을 대상과 함께 시작 순서로 다시
  만듭니다. 부모는 늘 먼저 시작한 이슈라 꼬리의 자식도 모두 꼬리 안에 있어 `story_root_id` 가 어긋나지
  않습니다. 꼬리는 지금부터 `NEWS_ISSUE_RELINK_WINDOW_HOURS` 안에 시작한 판정으로 묶습니다 — 그 밖의
  판정은 그대로 두므로, 그보다 오래된 이슈가 한꺼번에 들어오면(아래 "연결을 다시 만들어야 할 때")
  백필 스크립트로 다시 만듭니다.
- **관계**: 같은 사건이 클러스터 둘로 갈리는 일이 잦아 연결마다 관계를 붙입니다. 첫 기사 시각 차가
  `NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS` 이내이거나, 코사인이 `NEWS_ISSUE_SAME_EVENT_SCORE` 이상이면서
  시각 차가 `NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS` 이내이면 `same_event`(백엔드가 한 노드로
  접습니다), 아니면 `follow_up` 입니다. 코사인 쪽 간격 상한은 클러스터 시간 창
  `NEWS_CLUSTER_WINDOW_DAYS`(7일)와 같습니다 — 한 사건이 클러스터 둘로 갈리는 일은 그 창 안에서
  일어나고, 시리즈 지표의 다음 회차(7월 건설지출 → 8월 건설지출)는 제목·요약이 거의 같아 코사인이
  높아도 몇 주 떨어져 있습니다. 이 상한이 없으면 하한(0.75)이 같은 사건 기준과 같은 기업 없는 연결이
  전부 한 노드로 접힙니다.
- **실패**: 이슈 하나의 실패는 세고 넘어가 다음 런에 다시 봅니다. 전부 실패하면 task 를 실패시킵니다.
  연결 쓰기는 `updated_at` 을 건드리지 않습니다 — 승격·제목 재시도와 Event 스캔이 그 창을 봅니다.
- **DAG**: `summarize_articles` 뒤에 `trigger_rule="all_done"` 으로 돕니다. 요약이 실패해도 연결은
  돌고, 요약 실패는 요약 뒤의 말단 `finish` 가 런 실패로 남깁니다.

| 설정 | 기본값 | 의미 |
| --- | --- | --- |
| `NEWS_ISSUE_LINK_ENABLED` | `false` | 스케줄 연결 스위치. 백필 스크립트는 보지 않습니다 |
| `NEWS_ISSUE_LINK_THRESHOLD` | `0.45` | 주요 기업이 겹치는 부모의 코사인 하한 |
| `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD` | `0.75` | 기업 없는 이슈끼리의 하한 |
| `NEWS_ISSUE_LINK_LOOKBACK_DAYS` | `90` | 대상(지금부터)·부모 후보(대상 첫 기사부터) 범위 |
| `NEWS_ISSUE_LINK_MAX_PER_RUN` | `200` | 런당 대상 상한 |
| `NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS` | `24` | 같은 사건으로 보는 첫 기사 시각 차 |
| `NEWS_ISSUE_SAME_EVENT_SCORE` | `0.75` | 같은 사건으로 보는 코사인(아래 간격 상한 안에서만) |
| `NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS` | `168` | 코사인으로 같은 사건을 보는 첫 기사 시각 차 상한(클러스터 시간 창과 같은 7일) |
| `NEWS_ISSUE_RELINK_WINDOW_HOURS` | `72` | 꼬리 재판정 범위(지금부터, 시간). `0` 이면 다시 판정하지 않습니다 |
| `NEWS_ISSUE_EMBEDDING_MODEL` | `amazon.titan-embed-text-v2:0` | 연결용 임베딩 모델 |

임계값은 dev 데이터 시뮬레이션에서 고른 초기값입니다(0.6 은 같은 사건의 중복만 잇고 실제 후속
이슈를 놓쳤습니다). 라벨 평가셋으로 다시 맞춥니다. 임계값이나 임베딩 모델을 바꾸면 이미 저장된
판정·임베딩은 그대로이므로 백필 스크립트의 `--reset-links` 로 다시 만듭니다.

### 배포 순서

1. `migrations/versions/V12__issue_timeline.sql` 을 적용합니다(컬럼·인덱스만 추가).
2. 연결 설정을 끈 채(`NEWS_ISSUE_LINK_ENABLED=false`, 기본값) 배포합니다. `link_issues` 는 0 통계로 끝납니다.
3. 기존 이슈를 오래된 것부터 잇습니다. Airflow 에서 수동 DAG `news_backfill_issue_timeline` 을 실행합니다.
   기본값(`apply=false`)은 dry-run 이라 대상 수와 예시만 로그에 남기고, 쓰지도 Bedrock 을 부르지도
   않습니다. 확인한 뒤 `links=true, apply=true` 로 다시 실행합니다.

4. 백필이 끝나면 `NEWS_ISSUE_LINK_ENABLED=true` 로 켭니다.

설정을 먼저 켜면 lookback 안의 최근 이슈만 보고 판정하므로, 백필 전의 옛 이슈를 부모로 보지 못한 이슈가
루트로 남습니다. 그랬다면 설정을 끄고 `reset=true, links=true, apply=true`(기간을 좁히려면 `since_days`)로 다시
만듭니다. 초기화는 임베딩과 연결
컬럼만 지우고 `updated_at` 은 그대로 둡니다.

### 연결을 다시 만들어야 할 때

스케줄 연결은 재판정 기간 밖의 판정을 고치지 않습니다. 아래 작업 뒤에는 연결 설정을 끄고 백필
DAG `news_backfill_issue_timeline` 으로 그 기간의 연결을 다시 만든 뒤 켭니다.

- **옛 기사 백필(`news_backfill_krx100` → `news_backfill_cluster_articles`)**: 몇 달 전에 시작한 이슈가
  한꺼번에 생깁니다. `news_backfill_cluster_articles` 의 `link_issues` 는 실행당 상한만큼만, lookback
  안에서만 잇고, 그 뒤에 시작해 이미 판정된 이슈는 재판정 기간 밖이라 새 이슈를 부모로 보지 못합니다.
  백필 클러스터의 승격·제목·요약이 끝난 뒤(dry-run 로그의 "대표 요약 대기로 빠짐" 이 0) 백필 기간만큼
  `reset=true, links=true, since_days=<백필 기간 일수>, apply=true` 로 다시 만듭니다.

- **클러스터 재판정(`scripts/recluster_news.py --apply`)**: `news_clusters` 를 전부 지우므로 연결도 모두
  사라집니다. 연결 설정을 켠 채 두면 스케줄 연결이 lookback(90일) 안의 이슈만 승격 순서대로 다시
  잇고, 그보다 오래된 이슈는 판정하지 않습니다. `--apply` 전에 설정을 끄고, 재판정 뒤 승격·제목·요약이
  끝나면 백필 DAG 를 `links=true, apply=true` 로 실행해 전체를 이은 다음 켭니다(새 클러스터라 초기화는
  필요 없습니다).

## KRX100 백필 (dags/news/backfill_krx100.py)

`news_backfill_krx100` 은 초기 데이터를 채우는 수동 DAG 입니다. `stocks.krx100` 활성 종목의 기업
(`fetch_due_krx100_queries`)을 20개씩 청크로 나눠 하나씩 `collect_articles.run_krx100` 을 돌립니다.
대상 선정만 다르고 수집 이후 단계는 스케줄 런과 같은 `collect()` 를 씁니다. 검색어도 스케줄 런과
같은 `NEWS_SEARCH_QUERY_TEMPLATES` 입니다.

클러스터 판정은 청크마다 하지 않습니다. 모든 청크가 끝나면 `publish_backfill` 이
`etl://news/backfill-articles` 를 한 번 발행하고, 백필 전용 `news_backfill_cluster_articles`
(`dags/news/backfill_cluster_articles.py`)가 저장된 미판정 기사 전량을 한 번에 판정합니다. task 구성은
`news_cluster_articles` 와 같고 삼중항 DAG 도 같이 씁니다.

- **한 번에 판정하는 이유**: 시간 창의 기준점은 먼저 판정된 기사의 발행 시각이고, 그보다 이른 기사는
  1일 전까지만 붙습니다. 청크마다 판정하면 뒤 청크 기업의 기사가 앞 청크가 만든 클러스터보다 이르게
  도착해 같은 사건에 클러스터가 하나 더 생깁니다.
- **IDF**: 백필 판정은 저장된 IDF 표에 이번 배치의 문서 수를 더해 씁니다
  (`assign(include_batch_idf=True)`, `transformers/clustering/batch.py` 의 `add_batch_frequency`). 초기
  백필에는 표가 비어 있어, 그대로 쓰면 기업명 같은 흔한 토큰이 눌리지 않습니다.
- **발행 조건**: 실패한 청크가 있으면 발행하지 않습니다. 다시 트리거하면 남은 기업만 수집한 뒤
  발행합니다. 미판정 기사가 없으면 발행을 건너뜁니다.
- **백필 중에는 `news_cluster_articles` 를 꺼 둡니다.** 두 클러스터 DAG 모두 출처를 가리지 않고
  `cluster_id` 가 없는 기사 전량을 읽습니다. 켜 두면 정시 수집이 깨운 `news_cluster_articles` 가 수집
  중인 백필 기사를 먼저 판정하고, 두 DAG 가 겹쳐 돌면 같은 사건에 클러스터가 둘 생깁니다.
  `news_backfill_cluster_articles` 가 끝난 뒤 다시 켭니다.

- 수집 창은 트리거 params 로 넓힙니다: `lookback_days`(기본 180, 최대 180), `max_pages`(기본 8,
  최대 10). 스케줄 런은 `NEWS_SEARCH_LOOKBACK_DAYS`·`NEWS_SEARCH_MAX_PAGES` 를 그대로 씁니다.
- 워터마크·재검색 간격 규칙은 스케줄 런과 같습니다. 청크마다 `search_history` 를 마킹하므로
  중간에 실패해도 다시 트리거하면 끝난 기업은 건너뜁니다.
- 처음 검색하는 기업은 새 기사를 전부 LLM 관련성 필터에 보냅니다. 처음엔 `lookback_days` 를
  작게 줘서 청크당 기사 수와 소요 시간을 확인하세요.

## LLM 프롬프트 (transformers/prompts/)

- `relevance.py` — 관련성 판정. 입력은 기사마다 제목과 판정 기업뿐입니다. 판정 기업마다 실제 기업 지칭이면
  `is_company`, GATE 1(체결된 경제 관계)·GATE 2(기업 자체 사건)·GATE 3(기업 사업에 닿는
  구체적 재료, 기대감·전망 단계 포함) 중 하나를 통과하면 `valid`. 지수·수급 등 시장 전반 요인이나 타사
  재료로만 움직인 시세 기사는 탈락합니다.
- `cluster_title.py` — 클러스터 이름. 지시문은 토큰을 아끼려 영어로, 금지어 목록과 예시는 출력과 같은
  한국어로 둡니다.
- `entity.py` — 본문 기업 판정(엔티티 필터). 기업 지칭 여부와 구체적 사업 행위의 당사자 여부를 보고,
  애매하면 버립니다.
- `summary.py` — 기사 요약(`news_cluster_articles` 의 `summarize_articles` task). 승격된 클러스터의 대표
  기사 본문 하나로 해요체 요약 문단(`news.summary`, 3~4문장)과 핵심 포인트(`news.summary_points`)를
  만듭니다. 포인트는 `CHANGE`(무엇이 바뀌나요)·`AFFECTED`(누가 직접 영향받나요)·`SCALE`(얼마나
  큰가요)·`CAUSE`(왜 일어났나요)·`RIPPLE`(어디로 이어지나요) 중 본문이
  뒷받침하는 2~3개이고, 사건이 없는 기사는 빈 배열입니다. 모델은 키만 내고 화면 라벨은 키로
  매핑합니다. 전망은 본문에 적힌 것만 전망으로 읽히게 씁니다. 개수·중복·문체·길이는
  `transformers/summarizer.py` 가 검증해 어기면 한 번 재요청합니다. 길이 상한은 요약 4문장·문장당
  80자, 포인트 60자이고(같은 파일의 상수), 프롬프트에는 더 짧은 목표치(3문장·60자·45자)와 함께 들어갑니다.

판정·형식 기준은 시스템 프롬프트에만 둡니다. 구조화 출력 스키마(pydantic `Field` description·docstring)도
호출마다 LLM 에 실리는데, 스키마 안의 한글은 시스템 프롬프트보다 글자당 토큰이 몇 배 들어 같은 기준을
다시 적으면 입력이 크게 늘어납니다. 프롬프트를 고치면 고정 입력셋으로 전후 판정을 비교하세요 — 문구만
바꿔도 경계 사례의 판정이 움직입니다.

## 로컬 확인 (scripts/probe_news_to_triples.py)

검색어 하나로 네이버 검색 → 필터 → 관련성 판정 → 본문 크롤링 → 삼중항 추출까지 돌려 단계별 결과를
로그로 남깁니다. DB·Neo4j 에는 쓰지 않고(개체 사전만 읽음), 워터마크·DB 저장 URL 필터·클러스터 판정은
적용하지 않습니다. 네이버 검색 API 와 Bedrock 을 실제로 호출합니다.

```bash
uv run python scripts/probe_news_to_triples.py "두산에너빌리티,특징주" --top 10
```

로그는 `logs/probe_news_to_triples_<시각>.log` 에 남습니다(콘솔은 INFO, 파일은 DEBUG 까지).
