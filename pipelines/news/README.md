# News Pipeline

뉴스 수집·클러스터링·요약 파이프라인입니다. 기사 전문 저장은 피하고, URL/메타데이터/분석 결과 중심으로 저장합니다.

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
이어서 돕니다: `assign_clusters` → `promote_clusters` → `generate_events` ∥ `summarize_articles`.

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
