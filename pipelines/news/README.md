# News Pipeline

뉴스 수집·클러스터링·요약 파이프라인입니다. 기사 전문 저장은 피하고, URL/메타데이터/분석 결과 중심으로 저장합니다.

## 수집 원천

`jobs/collect_articles.run(theme_ids)` 가 받은 테마의 편입 종목을 기업 단위로 모으고
(`repositories/search_history.py`), 기업마다 `NEWS_SEARCH_QUERY_TEMPLATES`(기본 `{name},공급`·`{name},계약`·`{name},수혜`·`{name},호재`·`{name},악재`·`{name},특징주` 여섯 개)로
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
테마를 그대로 씁니다(`repositories/hot_themes.py`). 선정과 발행은 백엔드 몫이고
(`stocks_compute_derived` 의 핫테마 발행 트리거), ETL 은 직접 계산하지 않습니다.

폴백은 없습니다. 아래 경우 태스크가 실패합니다:

- Redis 조회 실패, 키 없음, 페이로드 파싱 실패
- 페이로드 `tradeDate` 가 null 이거나 DB 최신 거래일(캔들·밸류에이션이 둘 다 있는 최신 날짜,
  `repositories/trade_dates.py`)과 다름

수동 트리거 conf 로 `theme_ids` 를 주면 Redis 조회 없이 그 테마만 검색합니다:

```json
{"theme_ids": [12, 34]}
```

처음 검색하는 기업은 lookback 전체를 읽고 새 기사를 전부 LLM 에 보내므로, 테마는 한 번에
몇 개씩만 넘기세요.

## 단계 (jobs/collect_articles.py)

수집 → 제목 기업 매치(제목 × 기업 개체 사전, `transformers/company_matches.py`: 제목에 검색 종목이
없는 기사 제거, 제목의 상장사는 판정 대상으로) → 배치 URL 중복 제거(먼저 걸린 종목만 남김 — 매치가
먼저라 남는 사본은 검색 종목이 제목에 있는 것뿐) → 기사 유형 필터 → 제목 폴리싱 → DB 저장된 URL 제거(메모리 필터를 다 거친 뒤 DB 조회 1회) →
LLM 관련성 필터(제목·스니펫, 판정 기업마다, `transformers/filters/relevance_filter.py`) →
클러스터링·cap → 본문 크롤링 → 저장 → 클러스터 기록 → `news_companies` 연결 → `search_history` 갱신.

- LLM 필터가 클러스터링 앞에 있어 무관·시황 기사가 시드와 프로필을 오염시키지 않습니다.
- 기사 하나는 검색 대상 종목 하나에 속합니다(`_query_company`). **제목**에 검색 종목이 없으면
  버립니다. 언급은 문자열이 아니라 기업으로 확인합니다 — 개체 사전 매치 중 같은 `company_id` 가
  있어야 해서 약칭('LG엔솔')으로만 적힌 제목도 통과하고, 다른 상장사 이름의 일부('SK하이닉스' 안의
  'SK')는 언급으로 치지 않습니다. 매처는 대소문자를 구분합니다.
- LLM 은 제목에 나온 상장사(`판정 기업`, 검색 종목 첫 번째)마다 같은 기준(GATE 1/2)으로 `valid` 를
  냅니다. 저장 여부는 이 기사를 가져온 검색 종목 중 하나라도 통과했는지로 정합니다 — 같은 기사가
  여러 종목 검색에 걸리면 URL 중복 제거가 사본 하나로 합치며 걸린 검색 종목을 모두 기억합니다
  (`_query_companies`). 검색하지 않은 제목 기업만 통과하면 저장하지 않습니다. 호출은 `NEWS_LLM_BATCH_SIZE`(기본 10)건씩
  묶고, 응답은 기사 번호로 짝을 맞춥니다. 묶음이 실패하거나 번호·검색 종목 판정이 빠지면 그 기사만
  개별로 한 번 더 판정합니다.
- `news_companies` 는 수집 단계만 씁니다(`repositories/news_companies.py`). 저장된 기사를 판정을
  통과한 기업 전부에 연결합니다. 트리플 추출은 연결하지 않습니다. 스니펫·본문에만 나오는 기업은
  연결되지 않습니다.
- 버린 기사는 따로 기록하지 않습니다. 워터마크가 다음 런의 창 밖으로 밀어냅니다. 그래서
  LLM 호출 실패·cap 탈락·본문 실패 기사는 다시 오지 않습니다(전건 실패만 task 실패로 재시도).

## 하류 처리량

삼중항 추출과 요약은 수집 DAG 의 task 가 아니라 Asset 으로 이어지는 별도 DAG 입니다:
`collect_articles` ─► `etl://news/clusters` ─► `triples_extract_triples` ─► `etl://triples/extracted`
─► `news_summarize_articles` (이유는 `dags/README.md` 의 "Asset 의존" 참고).

`extract_triples`·`summarize_articles` 는 런 시작 시점의 미처리 기사를 상한 없이 전량 처리합니다.
실패한 기사는 미처리로 남아 다음 런(다음에 새 기사가 수집된 시점)에 다시 시도합니다 — 런 안에서 0건이 될 때까지 반복하지
않는 이유입니다(실패가 이어지면 끝나지 않습니다).

## KRX100 백필 (dags/news/backfill_krx100.py)

`news_backfill_krx100` 은 초기 데이터를 채우는 수동 DAG 입니다. `stocks.krx100` 활성 종목의 기업
(`fetch_due_krx100_queries`)을 20개씩 청크로 나눠 하나씩 `collect_articles.run_krx100` 을 돌립니다.
청크가 끝날 때마다 `etl://news/clusters` 를 발행해 삼중항 추출·요약이 수집과 나란히 진행됩니다. 대상 선정만 다르고 수집 이후 단계는 스케줄 런과 같은
`collect()` 를 씁니다. 검색어도 스케줄 런과 같은 `NEWS_SEARCH_QUERY_TEMPLATES` 입니다.

- 수집 창은 트리거 params 로 넓힙니다: `lookback_days`(기본 180, 최대 180), `max_pages`(기본 8,
  최대 10). 스케줄 런은 `NEWS_SEARCH_LOOKBACK_DAYS`·`NEWS_SEARCH_MAX_PAGES` 를 그대로 씁니다.
- 워터마크·재검색 간격 규칙은 스케줄 런과 같습니다. 청크마다 `search_history` 를 마킹하므로
  중간에 실패해도 다시 트리거하면 끝난 기업은 건너뜁니다.
- 처음 검색하는 기업은 새 기사를 전부 LLM 관련성 필터에 보냅니다. 처음엔 `lookback_days` 를
  작게 줘서 청크당 기사 수와 소요 시간을 확인하세요.
