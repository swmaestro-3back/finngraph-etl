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

이슈 하나(`news_clusters` 한 행이며 백엔드 `/api/v1/issues` 가 보여 줍니다)의 타임라인은 서로
이어지는 앞선 이슈들을 시작 순서대로 이은 것입니다(실적 프리뷰 → 실적 발표, 7월 건설지출 → 8월
건설지출). `link_issues` 는 이슈마다 부모 하나를 골라 `parent_cluster_id`, `story_root_id`(타임라인
첫 이슈의 id이며 루트는 자기 id), `link_score`(부모와의 코사인), `link_relation`, `linked_at` 을
씁니다. 백엔드는 요청한 이슈에서 `parent_cluster_id` 를 따라 올라간 이슈들을 최신순으로 보여 주고,
후속 이슈가 여러 갈래로 나뉜 경우 다른 갈래는 넣지 않습니다. `same_event` 로 이어진 이슈는 노드
하나로 합칩니다. 노드 제목은 `news_clusters.title` 이고, 한 줄 요약은 대표 기사 핵심 포인트의
`CHANGE` 항목(없으면 `news.summary` 첫 문장)입니다.

부모를 고르는 방식은 `NEWS_ISSUE_LINK_METHOD` 로 정합니다. 기본값인 `vote` 는 LLM 투표로 부모를
고르고, 주가 반응 이슈를 연결에서 뺍니다(아래 "투표 판정" 절). `cosine` 은 주요 기업 겹침과 코사인
임계값만 보는 규칙이며 LLM 을 부르지 않으므로, 문제가 생겼을 때 되돌리는 용도로 둡니다. 코사인 규칙은
`transformers/issue_linker.py` 에, 투표 판정은 `transformers/issue_link_vote/` 에 있습니다. 조회와 쓰기는
`repositories/postgres/news_clusters.py` 의 "이슈 타임라인" 절과 `repositories/postgres/issue_links.py`
에 있습니다.

- **대상**: 이름과 대표 기사가 있고 `linked_at` 이 NULL 인 클러스터를 첫 기사 시각순으로 고릅니다.
  지금부터 `NEWS_ISSUE_LINK_LOOKBACK_DAYS` 안에서 실행당 `NEWS_ISSUE_LINK_MAX_PER_RUN` 개까지입니다.
  판정마다 커밋하므로 같은 실행에서 앞선 대상이 뒤 대상의 부모 후보가 됩니다.
- **요약 대기**: 대표 기사에 요약(포인트나 문단)이 아직 없으면, 클러스터 `updated_at` 에서 24시간이
  지난 뒤에만 요약 없이 잇습니다. `updated_at` 은 승격과 제목 생성 때 갱신되므로, 그동안 클러스터 DAG
  가 실행될 때마다 요약을 다시 시도합니다. 후속 기사가 붙을 때도 갱신되므로 기사가 계속 붙는 이슈는
  그만큼 더 기다립니다. 대개 클러스터 기간(`NEWS_CLUSTER_WINDOW_DAYS`) 안에 끝납니다.
- **임베딩**: 제목, 한 줄 요약, 멤버 기사 제목 3개(후보 기사부터 발행 시각순)를 줄바꿈으로 이어
  Titan v2(1024)로 만들고 `news_clusters.embedding` 에 저장합니다. 후보 기사는 승격 전에 모두
  들어오고 그 뒤로 바뀌지 않으므로, 승격 직후에 만들든 기사가 더 붙은 뒤 백필에서 만들든 입력이
  같습니다. 모델은 테마용 `BEDROCK_EMBEDDING_MODEL` 과 별개인 `NEWS_ISSUE_EMBEDDING_MODEL` 입니다.
  한 번 저장한 임베딩은 다시 만들지 않습니다.
- **주요 기업**: 이슈의 `news_companies` 기업 가운데 멤버 기사 제목 하나 이상에 나온 기업입니다. 제목
  매치는 기사 수집 작업과 같은 개체 사전 매처(`common/gazetteer.py`)를 쓰므로 약칭도 같은 기업으로
  잡고, 본문에만 언급된 거래처나 경쟁사는 뺍니다. 제목에서 기업을 하나도 찾지 못하면 연결된 기업
  전체를 씁니다.
- **부모(`cosine` 방식)**: 대상보다 먼저 시작했고(첫 기사 시각, 같으면 id), 대상 시작 전 lookback 안이며, 이미
  판정된 이슈 가운데 주요 기업이 하나 이상 겹치고 코사인이 `NEWS_ISSUE_LINK_THRESHOLD` 이상인
  이슈입니다. 기업이 없는 이슈는 기업이 없는 이슈끼리만 `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD`
  이상으로 잇습니다. 코사인이 가장 큰 이슈가 부모이고, 같으면 늦게 시작한 쪽, 그래도 같으면 id 가 큰
  쪽입니다. 부모가 없으면 루트입니다. 후보는 코사인 순으로 30개씩 읽고, 주요 기업 규칙을 넘는 후보가
  나올 때까지 다음 페이지를 읽습니다. SQL 은 본문에만 언급된 기업까지 함께 거르므로, 그런 후보가 앞을
  채우면 맞는 부모가 뒤 페이지에 있을 수 있기 때문입니다.
- **재판정**: 이슈는 시작 순서가 아니라 승격과 요약을 받는 순서로 들어옵니다. 먼저 시작한 이슈가 늦게
  들어오면(후보 기사가 늦게 차거나 요약이 늦을 때), 그 뒤에 시작해 이미 판정된 이슈는 그 이슈를 부모로
  보지 못한 채 판정돼 있습니다. 예를 들어 같은 사건이 나뉜 두 클러스터가 각각 루트로 남습니다. 그래서
  실행마다 이번 대상 중 가장 먼저 시작한 이슈보다 뒤에 시작한 이슈(재판정 대상)를 대상과 함께 시작
  순서대로 다시 판정합니다. 부모는 늘 먼저 시작한 이슈이므로 재판정 대상의 자식도 모두 재판정 대상에
  들어 있고, 그래서 `story_root_id` 가 어긋나지 않습니다. 재판정 대상은 지금부터
  `NEWS_ISSUE_RELINK_WINDOW_HOURS` 안에 시작한 이슈로 한정합니다. 그 밖의 판정은 그대로 두므로, 그보다
  오래된 이슈가 한꺼번에 들어오면(아래 "연결을 다시 만들어야 할 때") 백필 DAG 로 다시 만듭니다.
  백필 DAG 는 재판정 대상을 고르는 범위를 재판정 기간이 아니라 백필 기간 전체로 넓힙니다. 실패하거나
  미룬 옛 대상이 뒤 배치에서 판정되면, 그보다 뒤에 시작해 앞 배치에서 판정된 이슈를 모두 다시 판정해야
  시작 순서대로 한 번에 이은 결과와 같아지기 때문입니다.
- **관계(`cosine` 방식)**: 같은 사건이 클러스터 둘로 나뉘는 일이 잦아서 연결마다 관계를 붙입니다. 첫 기사 시각 차가
  `NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS` 이내이거나, 코사인이 `NEWS_ISSUE_SAME_EVENT_SCORE` 이상이면서
  시각 차가 `NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS` 이내이면 `same_event`(백엔드가 노드 하나로
  합칩니다)이고, 아니면 `follow_up` 입니다. 코사인 쪽 간격 상한은 클러스터 기간
  `NEWS_CLUSTER_WINDOW_DAYS`(7일)와 같습니다. 한 사건이 클러스터 둘로 나뉘는 일은 그 기간 안에서
  일어나지만, 시리즈 지표의 다음 회차(7월 건설지출 → 8월 건설지출)는 제목과 요약이 거의 같아 코사인이
  높아도 몇 주 떨어져 있기 때문입니다. 이 상한이 없으면, 하한(0.75)이 같은 사건 기준과 같은 기업 없는
  연결은 모두 노드 하나로 합쳐집니다. 첫 기사 시각만 보므로 하루 안에 이어진 서로 다른 사건도
  `same_event` 로 합쳐질 수 있습니다.
- **실패**: 이슈 하나가 실패하면(LLM 오류 포함) 세고 넘어가며, 다음 실행에서 다시 판정합니다. 전부
  실패하면 task 를 실패시킵니다. 투표 판정의 LLM 호출 상한에 닿은 이슈는 실패가 아니라 미룸(`deferred`)
  으로 셉니다. 미룬 재판정 대상은 지난 판정을 둔 채 `linked_at` 만 비워 다음 실행의 대상으로 돌립니다.
  연결과 성격 분류를 쓸 때는 `updated_at` 을 바꾸지 않습니다. 승격·제목 재시도와 Event 스캔이
  `updated_at` 범위로 대상을 고르기 때문입니다.
- **DAG**: `summarize_articles` 뒤에 `trigger_rule="all_done"` 으로 실행합니다. 요약이 실패해도 연결은
  실행하고, 요약 실패는 요약 뒤의 마지막 task `finish` 가 실행 실패로 남깁니다.

| 설정 | 기본값 | 의미 |
| --- | --- | --- |
| `NEWS_ISSUE_LINK_ENABLED` | `false` | 스케줄 연결을 켜고 끄는 설정. 백필 DAG 는 이 값과 무관하게 실행됩니다 |
| `NEWS_ISSUE_LINK_THRESHOLD` | `0.45` | 주요 기업이 겹치는 부모의 코사인 하한 |
| `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD` | `0.75` | 기업 없는 이슈끼리의 코사인 하한 |
| `NEWS_ISSUE_LINK_LOOKBACK_DAYS` | `90` | 대상(지금부터)과 부모 후보(대상 첫 기사부터)를 찾는 기간 |
| `NEWS_ISSUE_LINK_MAX_PER_RUN` | `200` | 실행당 대상 상한 |
| `NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS` | `24` | 같은 사건으로 보는 첫 기사 시각 차 |
| `NEWS_ISSUE_SAME_EVENT_SCORE` | `0.75` | 같은 사건으로 보는 코사인(아래 간격 상한 안에서만) |
| `NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS` | `168` | 코사인으로 같은 사건을 판단하는 첫 기사 시각 차 상한(클러스터 기간과 같은 7일) |
| `NEWS_ISSUE_RELINK_WINDOW_HOURS` | `72` | 재판정 대상을 고르는 기간(지금부터, 시간). `0` 이면 다시 판정하지 않습니다 |
| `NEWS_ISSUE_EMBEDDING_MODEL` | `amazon.titan-embed-text-v2:0` | 연결용 임베딩 모델 |
| `NEWS_ISSUE_LINK_METHOD` | `vote` | 판정 방식. `vote`(LLM 투표) 또는 `cosine`(이전 규칙, 되돌리기용) |
| `NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN` | `1500` | 투표 판정이 실행 한 번에 새로 부르는 LLM 호출 상한(캐시 적중 제외) |
| `NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE` | `150` | 투표 판정이 이슈 하나에 실행마다 새로 부르는 LLM 호출 상한 |
| `BEDROCK_ISSUE_LINK_PROPOSER_MODEL` | `moonshotai.kimi-k2.5` | 제안자(screen, 렌즈, 계획 경로, rank) 모델 |
| `BEDROCK_ISSUE_LINK_CONFIRMER_MODEL` | `us.anthropic.claude-sonnet-4-6` | 확인자(judge, check, 계획 판독) 모델 |
| `BEDROCK_ISSUE_KIND_MODEL` | `us.anthropic.claude-sonnet-4-6` | 이슈 성격 분류의 확인 모델. 1차 답을 다시 판정할 때와 단일 모델로 분류할 때 씁니다 |
| `BEDROCK_ISSUE_KIND_SCREEN_MODEL` | `moonshotai.kimi-k2.5` | 이슈 성격 분류의 1차 모델. 비우거나 확인 모델과 같게 두면 확인 모델 하나로 분류합니다(`BEDROCK_CHAT_MODEL` 로 떨어지지 않습니다) |

투표 판정의 동시 호출 수는 다른 뉴스 LLM 단계와 같은 `NEWS_LLM_MAX_CONCURRENCY` 를 씁니다.

코사인 임계값은 dev 데이터로 고른 초기값입니다. 0.6 처럼 높이면 같은 사건의 중복만 잇고 실제 후속
이슈를 놓칩니다. 판정 방식, 임계값, 임베딩 모델을 바꿔도 이미 저장된 판정과 임베딩은 그대로이므로, 백필
DAG 를 `reset=true, links=true, apply=true` 로 실행해 다시 만듭니다.

### 투표 판정

제안자 둘이 후보를 내고 확인자 둘이 다시 확인하는 투표 방식입니다. 프롬프트 문자열은
`tests/news/test_issue_link_vote_prompts.py` 가 해시로 고정합니다. 프롬프트를 바꾸면 해시와
`llm.PROMPT_VERSIONS` 를 함께 올려야, 캐시에 남은 옛 응답을 다시 쓰지 않습니다.

1. **이슈 성격**: 이슈마다 한 번 `event` 와 `market_reaction` 중 하나로 분류해 `news_clusters.issue_kind`
   에 사유·모델·프롬프트 버전과 함께 남깁니다. `event` 는 회사 자신에게 일어난 일입니다. 공시, 발표된
   실적, 계약·수주, 투자, 인수합병·공개매수, 인허가, 소송·제재, 경영진 발언·노사 문제가 여기에 들고, 주가
   반응과 함께 보도돼도 `event` 입니다. 회사가 직접 확인하거나 공개한 새 사실(협의·회의 일정 확인, 자체
   참고자료·공시, 상용화 계획 발언) 때문에 주가가 뛰었다면, 계약 전이고 기사 제목이 '기대감'이라고 적어도
   `event` 입니다. 제목이 회사의 사실을 적고 기사 하나라도 그 사실을 보도하면, 나머지가 프리뷰나 주가 기사여도
   `event` 입니다. `market_reaction` 은 회사 자신의 새 사건 없이 주가·업종 흐름이나 전망만 있는 이슈입니다.
   외부 요인(거시 지표, 원자재 가격, 지정학, 정책 기대, 관료·정치인 발언, 테마, 수급, 협력사·계열사의
   사건), 증권사 리포트(목표가, 투자의견, 실적 추정·프리뷰), 회사가 확인하지 않은 기대(실적 발표 전 실적
   기대, 업계·익명 소식통이 전한 수주 협의), 외부 기관이 집계한 통계·순위(수출 통계, 조사기관 판매 순위),
   요즘 일어난 새 단계 없이 사업 방향과 이미 끝난 납품을 소개하는 기획 기사가 여기에 듭니다. 기사가 회사가
   요즘 새로 밟은 단계(새 계획 발표, 새 시장 진출, 전시회 출품, 공급 시작)를 보도하면 기획 기사로 보지 않고
   `event` 로 둡니다. 증권사 리포트가 숫자를 인용하거나 회사의 앞선
   사건을 다시 적어도 `market_reaction` 입니다. 판정은 이슈 제목과 기사 제목 다수를 기준으로 합니다. 요약은 대표 기사
   하나에서 나와 앞선 사실을 배경으로 다시 적는 일이 많아서, 입력에서 기사 제목 뒤에 둡니다. 프롬프트
   예시는 지어낸 회사 이름으로 만든 합성 제목입니다. `market_reaction` 이슈는 부모도 후보도 되지 않고,
   자기 자신은 루트로 판정합니다. 분류 응답을 끝내 읽지 못하면 가장 보수적인 답인 `market_reaction` 으로
   남기고 사유에 그 사실을 적습니다.

   분류는 두 모델이 나눠 맡습니다(`issue_link_vote/kind.py`). 1차 모델(Kimi K2.5)이 모든 이슈를 먼저
   분류하고, 아래 두 경우에만 확인 모델(Sonnet 4.6)이 같은 프롬프트로 다시 판정합니다. 확인 모델이
   판정하면 그 답이 최종 답이고, 그렇지 않으면 1차 답이 최종 답입니다.

   - 1차 답이 `market_reaction` 인 경우입니다. 이 답은 이슈를 타임라인에서 빼므로, 더 강한 모델이
     확인해야 회사 자신의 사건이 숨지 않습니다.
   - 1차 답이 `event` 이고, 이슈 제목이나 기사 제목에 리포트·전망 단서(`REPORT_CUES`: 목표가, 목표주가,
     투자의견, 증권, 리포트, 전망, 기대, 수혜, 컨센서스, 추정, 예상, 프리뷰)가 있는 경우입니다. Kimi 는
     실적 기대·증권사 리포트처럼 요약이 회사의 앞선 사실을 배경으로 적은 주가 반응 이슈를 `event` 로 읽는
     일이 잦습니다. 또 같은 프롬프트를 온도 0 으로 캐시 없이 다시 돌려도 판정이 1~2개씩 바뀌었습니다.

   dev 이슈 402개로 측정했을 때, Sonnet 하나로 분류하면 이슈 100개에 약 $1.26 이 들고 두 모델로 나누면
   약 $0.70 이 들었습니다. 확인 모델이 다시 판정한 이슈는 402개 중 173개(약 43%)였습니다. 블라인드로 만든
   정답과 비교하면 두 방식 모두 실제 사건을 주가 반응으로 숨긴 경우가 없었고, `market_reaction` 정밀도는
   둘 다 1.0, 재현율은 두 모델 방식 0.83, Sonnet 단독 0.85 였습니다.
   `news_clusters.issue_kind_model` 에는 최종 답을 낸 모델을 남깁니다. 1차 모델과 확인 모델의 응답은 둘 다
   `news_issue_link_llm_calls` 에 이슈 id 와 모델별로 남으므로, 1차 분류가 돌았는지는 그 표에서 확인합니다.
   `BEDROCK_ISSUE_KIND_SCREEN_MODEL` 을 비우거나 `BEDROCK_ISSUE_KIND_MODEL` 과 같게 두면 확인 모델
   하나로만 분류합니다.
2. **후보**: 대상보다 먼저 시작했고 90일(`NEWS_ISSUE_LINK_LOOKBACK_DAYS`) 안이며 판정이 끝난 `event`
   이슈 중 아래 하한을 넘는 것입니다. 연결 기업이 겹치면 코사인 0.10, 둘 다 기업이 없으면 0.50, 그 밖에는
   0.45 입니다. 아직 분류되지 않은 후보는 이때 분류합니다.
3. **제안자 둘**: pair 는 코사인 상위 12개 후보를 한 쌍씩 봅니다. screen 을 거친 뒤 evidence 렌즈 표본
   3개 중 2개와 matter 렌즈 표본 3개 중 2개가 받아들이면 통과합니다(route 1, 표본 0번은 온도 0, 1·2번은
   온도 1). route 1 이 부모를 하나도 주지 못한 대상에만 계획 경로(route 2)를 돌립니다. A 만 읽고 뽑은
   계획을 B 가 실행했는지 표본 3개가 모두 확인하고 코드 점검까지 통과해야 합니다. rank 는 주요 기업이
   겹치는 후보를 앞에 세운 10개를 한 번에 보여 주고, 행 라벨이 `SAME_EVENT`·`SAME_STORY` 이면서
   `check_affirm` 코드 점검을 통과한 후보를 제안합니다.
4. **확인자 둘**: 제안자가 받아들인 쌍에만 묻습니다. judge' 는 scope 판정(`scope-v4`)이 `SAME_EVENT`
   이거나 두 범위가 모두 `SPECIFIC` 인 `SAME_STORY` 일 때 통과하고, 실패하면 계획 판독(`plan-v5`)에
   묻습니다. 계획 판독은 `THE_ITEM` 이면서 두 인용문이 각자의 텍스트에 있고, 공통 대상이 기업 이름이
   아니며, 두 주체가 같아야 통과합니다. check(`scopecheck-v2`)는 `SAME_REPORT` 이거나, 두 쪽이 모두
   구체적 사안인 `NEW_STEP`·`NEXT_INSTALLMENT` 일 때 통과합니다. 프롬프트에 든 판정 예시 가운데
   판정할 쌍과 주요 기업이 겹치는 예시는 뺍니다. 예시마다 주요 기업 이름을 코드에 고정해 두고 비교합니다.
5. **결정**: judge' 와 check 가 모두 통과하고 제안자 하나 이상이 통과한 쌍만 연결 후보가 됩니다. 표가
   가장 많은 쌍, 그다음 `s = max(event, content)` 가 큰 쌍, 그다음 늦게 시작한 쌍, 그다음 큰 id 가
   부모입니다. content 는 운영 코사인이고, event 는 기업명을 가린 제목과 기사 제목으로 만든 점수입니다.
   `link_score` 에는 s 를 씁니다. 관계는 첫 기사 24시간 이내이거나 content 0.75 이상이면서 168시간
   이내이면 `same_event` 이고, judge·pair·rank 중 둘 이상이 `SAME_EVENT` 라고 해도 `same_event` 입니다.
   그 밖에는 `follow_up` 입니다.

후보별 표는 실행마다 `news_issue_link_votes` 에 남깁니다(묻지 않은 투표자는 NULL). LLM 응답은
`news_issue_link_llm_calls` 에 입력 해시(모델, 단계, 프롬프트 버전, 표본 번호, 요청)를 키로 저장하고,
뉴스 텍스트가 담긴 요청 원문은 저장하지 않습니다. 입력이 같으면 다음 실행(재판정 포함)이 저장된
응답을 그대로 쓰므로 새 호출이 생기지 않습니다. 검증에 실패한 응답은 짧은 수선 요청으로 한 번 더 묻고,
그래도 읽지 못하면 그 투표자는 통과하지 않은 것으로 봅니다. 도구 강제 호출을 지원하지 않는 모델은
도구 스키마를 메시지에 붙여 JSON 으로 받습니다(`issue_link_vote/llm.py` 의 `MODEL_CAPS`).

이미 `cosine` 방식으로 이은 판정은 방식을 바꿔도 그대로 남습니다. 투표로 다시 판정하려면 연결 설정
(`NEWS_ISSUE_LINK_ENABLED`)을 끄고 백필 DAG 를 `reset=true, links=true, apply=true` 로 실행해 다시
만듭니다.

LLM 새 호출은 실행당 상한과 이슈당 상한으로 제한합니다. 이슈당 상한에 닿으면 그 이슈만, 실행당
상한에 닿으면 남은 대상 전부를 다음 실행으로 미룹니다. 받은 응답은 캐시에 남으므로 다음 실행은 이어서
진행합니다. 재판정 대상을 미루면 그 이슈의 `linked_at` 만 비워(부모·루트는 다음 판정까지 그대로) 다음
실행의 대상이 되게 합니다. 다음 실행은 그 실행에서 가장 먼저 시작한 대상보다 뒤에 시작한 이슈만 재판정
대상으로 고르므로, 비우지 않으면 그보다 앞에 시작한 미룬 이슈를 다시 고르지 못합니다. 백필 DAG 의
`max_llm_calls` 는 배치마다 남은 호출 수를 실행당 상한으로 넘기므로 합계가 이 값을 넘지 않습니다.
이슈당 상한으로 미뤘더라도 실패 없이 새 호출을 한 배치는 진전으로 보고 다음 배치를 실행합니다. task
통계에는 `classified`, `market_reaction`, `kind_escalated`(1차 답을 확인 모델이 다시 판정한 수),
`proposed`, `confirmed`, `deferred`, `llm_calls`, `cache_hits`, `llm_cost_usd`(단가표로 추정한 비용이며
성격 분류의 두 모델을 모두 포함)가 더해집니다.

### 배포 순서

1. `migrations/versions/V12__issue_timeline.sql` 과 `V13__issue_link_votes.sql` 을 적용합니다(컬럼·
   테이블·인덱스만 추가합니다).
2. 연결 설정을 끈 채(`NEWS_ISSUE_LINK_ENABLED=false`, 기본값) 배포합니다. `link_issues` 는 0 통계로 끝납니다.
   `vote` 방식을 쓰려면 실행 환경의 Bedrock 계정에서 제안자·확인자·성격 분류의 1차·확인 모델(기본값은
   Kimi K2.5 와 Sonnet 4.6)을 호출할 수 있어야 합니다.
3. 기존 이슈를 오래된 것부터 잇습니다. Airflow 에서 수동 DAG `news_backfill_issue_timeline` 을 실행합니다.
   기본값(`apply=false`)은 dry-run 이라 대상 수와 예시만 로그에 남기고, 쓰지도 Bedrock 을 부르지도
   않습니다. 확인한 뒤 `links=true, apply=true` 로 다시 실행합니다. `vote` 방식은 이슈마다 LLM 을 여러
   번 부르므로, `max_llm_calls` 로 전체 새 호출 수를 제한해 여러 번에 나눠 실행할 수 있습니다. 멈췄다가
   다시 실행해도 받은 응답이 캐시에 남아 있으므로 이어서 진행합니다.

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
