# DAGs

Airflow DAG 정의 폴더. 각 하위 폴더는 **도메인**을 나타내며, Airflow는 이 폴더를 재귀적으로 스캔해서 `@dag`가 선언된 파일을 모두 DAG로 인식한다.

```
dags/
├── health/     # 운영 헬스체크
├── news/       # 뉴스 수집 · 필터 · 요약
├── stocks/     # 주가 캔들 수집 · 집계
├── themes/     # 테마 크롤링
└── triplets/   # 삼중항(관계) 추출
```

> 단, 폴더 구조는 **소스코드 정리용**이다. Airflow UI는 파일 경로가 아니라 `dag_id`와 `tags`로 DAG를 묶어 나열한다.

## DAG 목록

| 도메인 | 파일 | `dag_id` | `tags` | 스케줄 |
|--------|------|----------|--------|--------|
| companies | `companies/dart_pipeline.py` | `companies_dart_pipeline` | `companies` | `0 3 * * *` (03시) |
| companies | `companies/kis_financials.py` | `companies_kis_financials` | `companies` | `0 19 * * 1-5` (평일 19시) |
| companies | `companies/descriptions.py` | `companies_descriptions` | `companies` | `0 4 * * 6` (토 04시) |
| companies | `companies_sync_master.py` | `companies_sync_master` | `companies`, `master` | Asset ← `etl://stocks/master` |
| health | `health/healthcheck.py` | `health_check` | `health` | 수동 |
| news | `news/collect_headline.py` | `news_collect_headline` | `news` | `*/30 * * * *` (30분) |
| news | `news/collect_keyword_search.py` | `news_collect_keyword_search` | `news` | `0 * * * *` (매시) |
| news | `news/filter_meaningless.py` | `news_filter_meaningless` | `news` | Asset ← `etl://news/collected` |
| news | `news/summarize.py` | `news_summarize` | `news` | Asset ← `etl://news/relations` |
| stocks | `stocks_sync_master.py` | `stocks_sync_master` | `stocks`, `master` | `0 8 * * 1-5` (평일 08시) |
| stocks | `stocks/daily_pipeline.py` | `stocks_daily_pipeline` | `stocks` | `0 18 * * 1-5` (평일 18시) |
| stocks | `stocks/compute_derived.py` | `stocks_compute_derived` | `stocks` | Asset ← `etl://stocks/daily` **＋** `etl://companies/financials` |
| stocks | `stocks/weekly_dividends.py` | `stocks_weekly_dividends` | `stocks` | `0 6 * * 6` (토 06시) |
| stocks | `stocks/daily_backfill.py` | `stocks_daily_backfill` | `stocks` | 수동 |
| stocks | `stocks/intraday_1m.py` | `stocks_intraday_1m` | `stocks` | **정지** (분봉 수집 제외) |
| themes | `themes/etl.py` | `themes_etl` | `themes` | `0 0 * * *` (자정) |
| themes | `themes/link_news.py` | `themes_link_news` | `themes` | Asset ← `etl://news/relations` |
| triplets | `triplets/etl.py` | `triplets_etl` | `triplets` | Asset ← `etl://news/filtered` |

## Asset 의존

시각이 아니라 **앞 단계가 끝났다는 사실**에 걸어야 하는 작업이 있다. cron으로 못 박으면
앞 단계가 늦어지거나 실패한 날에도 그대로 돌아, 낡은 값을 섞은 결과가 조용히 나온다.

```
stocks_sync_master ──────────► etl://stocks/master ──────► companies_sync_master
                     (평일 08시)

stocks_daily_pipeline ───────► etl://stocks/daily ───┐
  (평일 18시, 일봉→기간봉→수급)                       ├──► stocks_compute_derived
companies_kis_financials ────► etl://companies/financials ┘      (PER·PBR·수익률)
  (평일 19시)

news_collect_* ──► etl://news/collected ──► news_filter_meaningless
                                              └─► etl://news/filtered ──► triplets_etl
                                                    └─► etl://news/relations ──► news_summarize
                                                                              └─► themes_link_news
```

`stocks_compute_derived`의 `schedule`은 **리스트라서 AND**다 — 두 Asset이 모두 갱신돼야
기동한다. PER은 분기 EPS 4개를 더한 TTM으로 계산하므로 시세와 재무가 모두 필요하다.

## `dag_id`

**Airflow 전체에서 DAG를 식별하는 고유한 이름.** 웹 UI 목록, CLI, 스케줄러, 실행 히스토리·메타데이터 DB가 모두 이 값을 키로 사용한다.

```python
@dag(
    dag_id="news_summarize",   # ← UI에 뜨는 이름, 전역 유일해야 함
    ...
)
```

### 규칙
- **전역 유일** — 두 DAG가 같은 `dag_id`를 쓰면 충돌한다.
  - 폴더가 이미 도메인을 나타내지만, UI는 평면(flat) 네임스페이스라 **`dag_id`에는 도메인 접두사를 유지**한다.
- **파일명과 일치시킨다** — UI에서 본 `dag_id`로 소스 파일을 바로 찾을 수 있어야 한다.
  - 예: `dag_id="news_summarize"` ↔ `dags/news/summarize.py`

### 주의 사항
`dag_id`를 바꾸면 Airflow는 **완전히 다른 새 DAG로 인식**한다. 기존 실행 히스토리·스케줄 상태가 UI에서 분리되므로, **운영 중인 DAG의 `dag_id`는 함부로 바꾸지 않는다.**

## `tags`

**Airflow UI에서 DAG를 필터링·그룹핑하기 위한 라벨.** 실행 로직에는 아무 영향이 없고, 순수하게 UI 탐색용이다.

```python
@dag(
    tags=["news"],   # ← UI 상단 태그 필터에서 "news" 클릭 시 이 DAG가 묶여 보임
    ...
)
```

### Tag 관련 규칙
- **태그는 도메인(폴더명) 하나만 사용한다.**
  - `["news"]`, `["stocks"]`, `["themes"]`, `["triplets"]`, `["health"]`
- 세부 동작명(`headline`, `summarize` 등)은 **넣지 않는다.**
  - 이미 `dag_id`에 담겨 있어 중복이고, 한 번만 쓰이는 태그가 늘어나 UI만 지저분해진다.
- 태그는 **여러 DAG이 공유하며 그것으로 걸러 볼 가치가 있을 때만** 의미가 있다. 도메인이 바로 그 축이다.