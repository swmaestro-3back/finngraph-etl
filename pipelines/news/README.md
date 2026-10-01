# News Pipeline

뉴스 수집·클러스터링·요약 파이프라인입니다. 선별(LLM 관련성 필터·클러스터 cap)을 통과한 기사만
본문을 크롤링하고, 정제한 본문을 `news.text` 에 저장합니다. 기사 요약·삼중항 추출과 클러스터 이름·요약이 이 본문을
읽습니다(클러스터 이름·요약은 기사당 앞 600자 리드만).

## 수집 원천

`jobs/collect_articles.run(theme_ids)` 가 받은 테마의 편입 종목을 기업 단위로 모으고
(`repositories/search_history.py`), 기업마다 `NEWS_SEARCH_QUERY_TEMPLATES`(기본 `특징주,{name}`·`{name}` 두 개)로
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
필터(제목·스니펫, `transformers/filters/relevance_filter.py`) → 클러스터링·cap → 본문 크롤링 →
저장 → 클러스터 기록·판정 기록 → 클러스터 이름·요약 → `news_companies` 연결 → `search_history` 갱신.

- LLM 필터가 클러스터링 앞에 있어 무관·시황 기사가 시드와 프로필을 오염시키지 않습니다.
- LLM 은 `valid`(종목 페이지에 보여줄 가치)와 `companies`(후보 중 기사가 다루는 상장사)를
  구조화 출력으로 냅니다. 후보 밖 이름은 버립니다.
  호출은 `NEWS_LLM_BATCH_SIZE`(기본 10)건씩 묶고, 응답은 기사 번호로 짝을 맞춥니다. 묶음이
  실패하거나 번호가 빠지면 그 기사만 개별로 한 번 더 판정합니다.
- `news_companies` 는 LLM 이 고른 상장사명을 `stocks.name → company_id` 로 해석해 연결합니다
  (`repositories/news_companies.py`). 트리플 추출이 같은 기사에서 다른 상장사를 찾으면 그 행도
  추가됩니다.
- 버린 기사는 다시 처리하지 않습니다. 워터마크가 다음 런의 창 밖으로 밀어냅니다. 그래서
  LLM 호출 실패·cap 탈락·본문 실패 기사는 다시 오지 않습니다(전건 실패만 task 실패로 재시도).
- 클러스터 판정은 LLM 통과 기사마다 `news_cluster_judgments` 에 한 행씩 남깁니다(cap 탈락·본문
  실패 포함, `transformers/clustering/judgments.py`). `cluster_id` 는 판정된 클러스터(새 클러스터를
  만들지 않았으면 NULL), `kept` 는 실제 멤버가 됐는지(cap 안 + 저장 + 클러스터 기록 성공),
  `seed_similarity` 는 기존 클러스터 합류일 때 시드 프로필과의 유사도입니다. cap 탈락과 본문 실패는
  둘 다 `kept=false` 라 이 테이블만으로는 구분하지 않습니다. 평가용이라 기록이 실패해도 런은
  경고만 남기고 계속합니다.
- 판정 기사 수가 `NEWS_CLUSTER_TITLE_MIN_SIZE` 이상인데 이름이 없는 클러스터는 LLM 한 번으로
  이름(`title`, `NEWS_CLUSTER_TITLE_MAX_CHARS`)과 해요체 1~2문장 요약(`summary`,
  `NEWS_CLUSTER_SUMMARY_MAX_CHARS` 기본 150)을 함께 받습니다(`transformers/cluster_titler.py`).
  입력은 저장된 멤버 기사의 제목과 본문 리드 600자입니다. 요약이 비었거나 길면 요약만 NULL 로
  두고 이름은 저장합니다.

## 이슈 타임라인 (jobs/link_issues.py)

`news_scheduled_pipeline` 의 `link_issues` 태스크가 `collect_articles` 뒤에 삼중항·요약 체인과
나란히 돕니다(`all_done` — 수집이 스킵돼도 밀린 클러스터를 처리). `NEWS_ISSUE_LINK_ENABLED`(기본
false)가 꺼져 있으면 아무것도 하지 않습니다. 켜져 있으면 이름이 있고 `linked_at` 이 NULL 이며 지금부터
`NEWS_ISSUE_LINK_LOOKBACK_DAYS` 안에 시작한 클러스터를 오래된 순으로 `NEWS_ISSUE_LINK_MAX_PER_RUN`(기본
200)개 골라:

1. 임베딩이 없으면 `이름\n요약\n최신 기사 제목 3개`를 Titan v2(1024, `NEWS_ISSUE_EMBEDDING_MODEL`)로
   한 번에 임베딩해 `news_clusters.embedding` 에 저장합니다.
2. 대상마다 먼저 시작했고 `NEWS_ISSUE_LINK_LOOKBACK_DAYS`(기본 90) 안이며 이미 판정된 클러스터 중
   기업(멤버 기사의 `news_companies`)이 겹치고 코사인이 `NEWS_ISSUE_LINK_THRESHOLD`(기본 0.6) 이상인
   것을 부모로 고릅니다(최대 코사인, 같으면 더 늦게 시작한 쪽). 기업이 없는 클러스터는 기업 없는
   클러스터에만 `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD`(기본 0.75) 로 잇습니다. 규칙은
   `transformers/issue_linker.py`.
3. `parent_cluster_id`, `story_root_id`(부모의 루트, 없으면 자기 id), `link_score`, `linked_at` 을
   씁니다. 판정마다 커밋하므로 같은 런의 뒤 대상은 앞 대상을 후보로 봅니다.

임계값은 초기값이라 라벨 평가셋(`scripts/eval/issue_links`)으로 다시 맞춥니다. 스키마는
`migrations/versions/V6__issue_timeline.sql`.

### 반영 순서 (지켜야 함)

한 번 저장한 임베딩과 연결 판정은 다시 만들지 않으므로 순서가 틀어지면 그대로 굳습니다.

1. **V6 적용** — develop 머지 전에. develop 푸시가 바로 배포되는데, V6 없이 배포되면
   `collect_articles` 의 이름·요약 저장(13단계)이 실패해 그 뒤 `news_companies` 연결과
   `search_history` 갱신까지 못 합니다.
2. **배포** — `NEWS_ISSUE_LINK_ENABLED` 는 false 그대로. 새로 이름이 붙는 클러스터는 요약을 같이 받습니다.
3. **요약 백필** — `scripts/backfill_issue_timeline.py --summaries --apply`. 배포 전에 이름이 붙은
   클러스터에 요약을 채웁니다. 연결보다 먼저여야 임베딩에 요약이 들어갑니다.
4. **연결 백필** — `--links --apply` 를 대상이 없어질 때까지. 기본이 전체 기간이라 lookback(90일)보다
   오래된 클러스터도 잇습니다. 스케줄 연결은 lookback 안만 보므로, 이걸 건너뛰면 옛 클러스터를 부모로
   못 봐 최근 클러스터가 루트로 굳습니다.
5. **스위치 켜기** — `NEWS_ISSUE_LINK_ENABLED=true`. 이후 새 클러스터는 스케줄 연결이 잇습니다.

백필은 기본이 dry-run(쓰기·LLM·Bedrock 호출 없음)이고 스위치와 무관하게 돕니다. 순서가 틀어졌으면
스위치를 끄고 `--reset-links --summaries --links --apply` 로 임베딩·연결 판정을 지운 뒤 다시 만듭니다
(`--since-days N` 이면 최근 N일에 시작한 클러스터만 — 부모는 늘 더 먼저 시작한 클러스터라 그 앞의
판정은 그대로 맞습니다).
