# News Pipeline

뉴스 수집·클러스터링·요약 파이프라인입니다. 기사 전문 저장은 피하고, URL/메타데이터/분석 결과 중심으로 저장합니다.

## 수집 원천

`jobs/collect_articles.run(theme_ids)` 가 받은 테마의 편입 종목을 기업 단위로 모으고
(`repositories/search_history.py`), 기업마다 `NEWS_SEARCH_QUERY_TEMPLATE`(기본 `특징주,{name}`)로
검색어를 만들어 네이버 뉴스 검색 API 를 최신순으로 호출합니다
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

런마다 `stock_candles_daily` 의 최신 거래일 기준으로 급등락 테마를 고릅니다. 종목 등락률은
그 날 종가 / 직전 거래일 종가 − 1, 테마 등락률은 편입 종목 등락률의 단순 평균입니다
(`repositories/theme_changes.py`, 봉이 있는 종목 3개 미만인 테마는 제외). 상승 상위 절반 +
하락 상위 절반으로 `NEWS_THEME_COUNT`(기본 40)개를 뽑습니다
(`transformers/theme_momentum.py`, 프론트 트리맵과 같은 규칙).

수동 트리거 conf 로 `theme_ids` 를 주면 계산 없이 그 테마만 검색합니다:

```json
{"theme_ids": [12, 34]}
```

처음 검색하는 기업은 lookback 전체를 읽고 새 기사를 전부 LLM 에 보내므로, 테마는 한 번에
몇 개씩만 넘기세요.

## 단계 (jobs/collect_articles.py)

수집 → 배치 URL 중복 제거(출처 종목 병합) → 기사 유형 필터 → DB 저장된 URL 제거 → 후보 상장사
부착(`transformers/company_candidates.py`, 검색 종목 ∪ KRX gazetteer) → LLM 관련성
필터(제목·스니펫, `transformers/relevance_filter.py`) → 클러스터링·cap → 본문 크롤링 →
저장 → 클러스터 기록 → `news_companies` 연결 → `search_history` 갱신.

- LLM 필터가 클러스터링 앞에 있어 무관·시황 기사가 시드와 프로필을 오염시키지 않습니다.
- LLM 은 `valid`(종목 페이지에 보여줄 가치)와 `companies`(후보 중 기사가 다루는 상장사)를
  구조화 출력으로 냅니다. 후보 밖 이름은 버립니다.
  호출은 `NEWS_LLM_BATCH_SIZE`(기본 10)건씩 묶고, 응답은 기사 번호로 짝을 맞춥니다. 묶음이
  실패하거나 번호가 빠지면 그 기사만 개별로 한 번 더 판정합니다.
- `news_companies` 는 LLM 이 고른 상장사명을 `stocks.name → company_id` 로 해석해 연결합니다
  (`repositories/news_companies.py`). 트리플 추출이 같은 기사에서 다른 상장사를 찾으면 그 행도
  추가됩니다.
- 버린 기사는 따로 기록하지 않습니다. 워터마크가 다음 런의 창 밖으로 밀어냅니다. 그래서
  LLM 호출 실패·cap 탈락·본문 실패 기사는 다시 오지 않습니다(전건 실패만 task 실패로 재시도).
