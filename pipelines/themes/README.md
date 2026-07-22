# Themes Pipeline

주식 테마(테마주) 데이터를 여러 소스에서 크롤링해 Neo4j GraphDB에 적재하는 파이프라인입니다.

## 개요

`naver`, `judal`, `antwinner` 3개 소스에서 테마명과 소속 종목을 크롤링(Extract) →
Neo4j에 이미 존재하는 테마/종목과 대조해 검증 및 병합(Validate) →
Neo4j에 `Theme`, `Company` 노드와 `BELONGS_TO` 관계로 적재(Load)하는 구조입니다.

## 디렉터리 구조

```
pipelines/themes/
  models.py          # Theme, Company pydantic 모델
  crud.py            # Neo4j 쿼리
  validator.py       # 검증/병합 로직
  loader.py          # Neo4j 적재
  extractors/         # 소스별 크롤러 (base, naver, judal, antwinner, factory)
  jobs/               # Airflow task가 호출하는 진입점 (run_pipeline.py)
  data/               # 소스별 크롤링 결과 JSON (날짜별 폴더)
```

## 파이프라인

```
extract (per source) -> save(JSON) -> validate(company/theme 병합) -> load(Neo4j)
```

- **Extract**: `extractors/{naver,judal,antwinner}.py` — 각 소스별 `BaseExtractor` 구현체.
  - `fetch_themes()`: 블랙리스트 제외한 테마명 크롤링
  - `extract_theme_stock()`: 테마별 소속 종목 크롤링
  - `extract()` 결과는 `data/{YYYYMMDD}/{source}.json`으로 저장
  - `extractors/factory.py`의 `ExtractorFactory.get_extractor(source_name)`으로 소스명에 맞는 Extractor를 팩토리 메서드 패턴으로 생성
- **Validate**: `validator.py`
  - `validate_company`: 크롤링한 종목명/ticker를 Neo4j 기존 `Company`와 대조, 불일치·미존재 종목 제외
  - `validate_theme`: 이름 동일 또는 이름 포함 관계 + 종목 overlap(임계값 0.9) 기준으로 기존/배치 내 테마와 병합
- **Load**: `loader.py` → `crud.py`의 `upsert_themes`로 Neo4j에 반영

`jobs/run_pipeline.py`의 `run()`이 Airflow task 및 로컬 실행의 진입점입니다.

## 로컬 실행

```bash
python -m pipelines.themes.jobs.run_pipeline
# 또는
python scripts/run_job.py pipelines.themes.jobs.run_pipeline:run
```

실행 전 `.env`에 `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`를
설정해야 합니다 (`pipelines/common/config.py` 참고).
