# 이슈 연결 임계값 평가

이슈 타임라인은 새로 이름이 붙은 클러스터를 회사를 공유하는 이전 클러스터 중 임베딩 코사인이 가장 높은
것에 잇는다(임계값 이상일 때만). 회사가 없는 클러스터는 회사 없는 이전 클러스터에만 더 높은 임계값으로 잇는다.
여기 스크립트는 그 두 임계값을 고르기 위한 한국어 골드셋을 만든다.
LLM 이 먼저 라벨을 달고, 사람이 300쌍 남짓을 검수한 뒤 임계값별 precision/recall 을 계산한다.

| 파일 | 역할 |
| --- | --- |
| `dump_candidates.py` | dev DB 에서 클러스터를 읽어 임베딩하고, 후보 쌍(회사 공유 / 둘 다 회사 없음)을 점수 밴드별로 층화 추출한다 |
| `prelabel.py` | 쌍마다 LLM 사전 라벨(`SAME_EVENT` / `SAME_STORY` / `DIFFERENT`)과 이유 한 줄을 단다 |
| `score.py` | 검수된 CSV 로 임계값 0.30~0.90 의 가중 precision/recall/F1 과 LLM-사람 일치도를 낸다 |
| `GUIDELINE.md` | 라벨 기준. 사람 검수자가 읽고, `prelabel.py` 가 같은 내용을 프롬프트로 쓴다 |
| `linkeval.py` | 세 스크립트가 함께 쓰는 상수(밴드, 라벨, CSV 열)와 임베딩 캐시·파일 입출력. 임베딩 입력은 운영 `issue_linker.build_embedding_text` 를 그대로 쓴다 |

## 실행 순서

레포 루트(`etl/`)에서 실행한다.

### 1. dev 터널 열기

```bash
scripts/dev-tunnel.sh start          # 팀원 사본(.gitignore 대상). postgres → localhost:15432
export DATABASE_URL='postgresql+psycopg://etl_app:<비밀번호>@localhost:15432/finngraph_dev?sslmode=require'
```

비밀번호는 `scripts/dev-env.sh` 가 만든 `.env.dev` 의 `DB_PASSWORD` 를 쓴다. 호스트만 터널(`localhost:15432`)로
바꾼다. 셸에서 export 한 값이 `.env` 보다 우선하므로 로컬 `.env` 는 고치지 않아도 된다.

### 2. 후보 쌍 추출

```bash
.venv/bin/python scripts/eval/issue_links/dump_candidates.py --since-days 90 --lookback-days 90 --per-band 60 --per-band-no-company 30 --seed 42
```

- B(나중 클러스터)는 최근 `--since-days` 일 안에 시작한 것, A 는 B 보다 먼저 시작해 `--lookback-days` 일 이내인 것이다.
  그래서 DB 에서는 `since + lookback` 일치 클러스터를 읽는다.
- 회사 공유 쌍은 코사인 밴드 `[0,0.4) [0.4,0.5) [0.5,0.6) [0.6,0.7) [0.7,0.8) [0.8,1]` 마다 `--per-band` 개씩 뽑고,
  밴드별 모집단 크기를 `band_population` 에 적는다.
- 둘 다 회사가 없는 쌍(운영의 회사 없는 연결 후보)은 같은 밴드로 `--per-band-no-company`(기본 30)개씩 따로 뽑는다.
  밴드 이름은 `no_company:0.8-1.0` 처럼 앞에 `no_company:` 가 붙는다.
- 어느 후보에도 들지 않는 나머지 쌍은 `--no-shared`(기본 30)개만 무작위로 뽑아 `no_shared` 층으로 둔다.
- 행 순서는 섞여 있다. 행 순서로 점수를 짐작하지 못하게 하려는 것이다. `pair_id` 는 `<a_id>_<b_id>` 로 고정이다.
- 결과는 `tmp/eval/issue_links/` 에 생긴다.

| 출력 | 내용 |
| --- | --- |
| `pairs.csv` | 검수용 시트. BOM 을 붙인 UTF-8 이라 Excel 에서 바로 열린다 |
| `pairs.jsonl` | 같은 쌍 + 회사 목록·간격 일수 등 부가 정보. `prelabel.py` 입력 |
| `meta.json` | 실행 파라미터, 밴드별 모집단·표본 수, 클러스터 수(회사 없음·요약 있음 포함) |
| `embedding_cache.jsonl` | 클러스터 id + 입력 텍스트 해시 → 벡터. 재실행하면 바뀐 클러스터만 다시 임베딩한다 |

### 3. LLM 사전 라벨

```bash
.venv/bin/python scripts/eval/issue_links/prelabel.py --limit 10    # 먼저 10쌍으로 라벨·이유를 눈으로 확인
.venv/bin/python scripts/eval/issue_links/prelabel.py               # 나머지. 끊기면 다시 실행하면 이어서 한다
```

결과는 `llm_labels.jsonl` 에 한 쌍씩 바로 쌓이고, 끝나면 `pairs.jsonl` · `pairs.csv` 의 `llm_label` · `llm_reason` 을
채운다. `pairs.csv` 에 이미 적힌 `human_label` · `human_note` 는 보존한다. 코사인 점수는 프롬프트에 넣지 않는다.

### 4. 사람 검수

`GUIDELINE.md` 를 읽고 `pairs.csv` 를 Excel 이나 Google Sheets 에서 연다. `human_label` 에 `SAME_EVENT` /
`SAME_STORY` / `DIFFERENT`(줄임말 `E` / `S` / `D`)를 채운다. 판단할 수 없으면 `?` 를 쓰거나 비워 둔다.
저장은 **CSV UTF-8** 로 한다. 다른 이름(`pairs_reviewed.csv` 등)으로 저장해도 된다.

### 5. 채점

```bash
.venv/bin/python scripts/eval/issue_links/score.py --csv tmp/eval/issue_links/pairs.csv
```

출력:

- 회사 공유 쌍과 회사 없는 쌍 각각에 대해:
  - 임계값 0.30~0.90 (0.05 간격)별 precision / recall / F1, 라벨된 표본 중 예측 양성 수(`n_pred`),
    모집단에서 그 임계값을 넘는 후보 쌍 수 추정(`est_pairs`)
  - **F1 최대** 임계값과 **precision ≥ 0.9 중 recall 최대** 임계값, 그리고 각각의 bootstrap 95% 구간
- 밴드별 라벨 분포, `no_shared` 층으로 본 '후보 규칙 때문에 놓치는 연결' 추정
- LLM-사람 일치도: 3분류와 양성/음성 각각의 accuracy · Cohen's kappa, 혼동행렬

양성은 `SAME_EVENT` 와 `SAME_STORY` 다. 채점은 두 후보 모집단을 따로 하고 `no_shared` 는 뺀다. 행마다
`band_population / 그 밴드에서 라벨된 행 수` 를 가중치로 줘 층화 표본을 모집단 기준으로 되돌린다
(Horvitz-Thompson). 잘못 이어진 타임라인이 빠진 연결보다 눈에 잘 띄므로, 기본으로는 precision ≥ 0.9 쪽 임계값을
권한다. 회사 공유 쪽 값은 `NEWS_ISSUE_LINK_THRESHOLD`, 회사 없는 쪽 값은 `NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD` 에
반영한다.

옵션: `--min-precision`(기본 0.9), `--bootstrap`(기본 1000, 0 이면 생략), `--seed`.

## 환경 변수

| 변수 | 쓰는 곳 | 비고 |
| --- | --- | --- |
| `DATABASE_URL` | dump | 터널 경유 dev DB. 읽기 전용 트랜잭션으로 SELECT 만 한다 |
| `BEDROCK_REGION` | dump, prelabel | |
| `AWS_BEARER_TOKEN_BEDROCK` | dump, prelabel | 또는 AWS 프로필 자격증명 |
| `NEWS_ISSUE_EMBEDDING_MODEL` | dump | 운영 이슈 연결과 같은 설정을 읽는다(기본 `amazon.titan-embed-text-v2:0`). 운영 배포 환경의 값과 같게 둔다. 테마용 `BEDROCK_EMBEDDING_MODEL` 은 쓰지 않는다. 모델은 `meta.json` 과 캐시 키에 남는다 |
| `BEDROCK_CHAT_MODEL` | prelabel | news 관련성 필터·클러스터 이름 생성과 같은 모델 |
| `BEDROCK_REQUEST_TIMEOUT` | dump, prelabel | 선택. 기본 300초 |

## 비용

- **임베딩**: 호출 수 = 읽은 클러스터 수(첫 실행만). 캐시 덕에 재실행은 이름·요약·기사 제목이 바뀐 클러스터만
  다시 부른다. 클러스터당 입력이 300자 안팎으로 짧아 LLM 사전 라벨 비용에 비하면 무시할 만하다.
- **LLM 사전 라벨**: 호출 수 = 쌍 수. 기본값이면 최대 570회(6밴드 × 60 + 6밴드 × 30 + 30)이고, 모집단이 표본 수보다
  작은 밴드는 그만큼 준다. 호출당 입력은 가이드 약 2천 토큰 + 쌍 0.5~1천 토큰, 출력은 100 토큰 안쪽이다. 570회면 입력
  약 150만 토큰, 출력 약 6만 토큰이므로 `BEDROCK_CHAT_MODEL` 의 토큰 단가를 곱해 가늠한다. 이미 라벨된 쌍은 다시 묻지 않는다.
- **채점**: Bedrock 을 부르지 않는다.

## 뉴스 본문은 어디에 쓰이나

- 임베딩 입력(이름 + 요약 + 최신 기사 제목 3개)에 본문을 직접 넣지는 않는다. 요약은 클러스터 이름 생성 호출이 멤버
  기사의 본문 리드(기사당 600자)를 읽고 만든 1~2문장이라, 본문 정보는 요약을 거쳐 들어간다.
- 검수와 사전 라벨에는 대표 기사 본문 리드(`--lead-chars`, 기본 200자)를 `a_lead` · `b_lead` 로 같이 보여준다.
  제목만으로 판단하기 어려운 쌍을 위한 것이고, 채점에는 쓰지 않는다.
- 본문을 임베딩에 넣을지도 이 골드셋으로 검증할 수 있다. 사람 라벨은 임베딩 방식과 무관하고, 표본 추출 확률은 원래 밴드에서
  정해지므로 같은 쌍을 다른 입력으로 다시 점수 매겨도 같은 가중치로 채점할 수 있다. 다만 그 재채점 스크립트는 아직 없다.

## 주의

- **recall 은 후보 모집단 안에서의 재현율이다.** 분모는 둘 다 이름이 있고, lookback 안이고, 회사를 공유하는(또는 둘 다
  회사가 없는) 쌍뿐이다. 어느 후보도 되지 못한 연결은 빠져 있고, 그 규모는 `no_shared` 층으로만 거칠게 가늠한다.
- **쌍 단위 규칙과 운영 규칙은 다르다.** 채점은 'score ≥ t 이면 연결'을 쌍마다 따지지만, 운영은 임계값을 넘는 후보 중
  점수가 가장 높은 하나만 부모로 삼는다. 그래서 운영의 연결 정확도는 쌍 단위 precision 과 다를 수 있다.
- **요약 반영 시점.** 마이그레이션 전이거나 요약이 아직 비어 있으면 임베딩 입력이 이름 + 기사 제목뿐이라 운영과 점수
  분포가 다르다(dump 로그와 `meta.json` 의 `summary_column` · `clusters_with_summary` 로 확인). 요약이 채워진 뒤 dump 를
  다시 돌리면 바뀐 클러스터만 다시 임베딩된다. 점수가 바뀌면 같은 `--seed` 라도 표본이 달라지므로, 이미 단 사람 라벨은
  `pair_id` 로 옮겨야 한다.
- **임베딩 입력 형식과 모델은 운영과 같다.** `linkeval.embedding_text` 는 운영 `issue_linker.build_embedding_text` 이고,
  모델도 운영과 같은 `NEWS_ISSUE_EMBEDDING_MODEL` 을 읽는다. 운영 쪽 형식이나 모델을 바꾸면 dump 부터 다시 돌린다.
- **검수 편향.** 검수 중에는 `score` · `band` 열을 숨긴다. LLM 라벨을 보며 단 행은 kappa 가 부풀려지므로, 일치도를
  정직하게 재려면 처음 일부는 `llm_label` · `llm_reason` 을 숨기고 단다.
- **표본 크기.** 밴드당 60쌍이면 밴드 안 양성 비율의 95% 오차가 최대 ±13%p 쯤이다. 점추정만 보지 말고 bootstrap
  구간과 `n_pred` 를 같이 본다.
- **건너뛴 행.** 비워 둔 행은 밴드 안에서 무작위로 빠졌다고 보고 가중치를 계산한다. 어려운 쌍만 골라 건너뛰면 편향된다.
- **회사 없는 클러스터.** 운영은 회사 없는 클러스터끼리만, 더 엄격한 임계값으로 잇는다. 이 쌍은 `no_company:` 밴드로
  따로 층화하고 따로 채점한다. 모집단(`meta.json` 의 `no_company_pairs`)이 작으면 표본도 그만큼 작아 구간이 넓다.
- **데이터 취급.** 출력에 기사 제목과 본문 리드가 들어간다. `tmp/` 는 레포 `.gitignore` 에 없어서 dump 가 출력 폴더에
  `*` 만 적힌 `.gitignore` 를 만든다. 결과 파일은 커밋하거나 외부에 공유하지 않는다.
- **DB 는 계속 바뀐다.** 같은 옵션이라도 실행 시점에 따라 표본이 다르다. `meta.json` 을 결과와 함께 보관한다.

## 테스트

```bash
.venv/bin/python -m pytest tests/eval -q
```

DB·Bedrock 없이 가짜 데이터로 후보 생성, 층화 추출, 가중 채점, kappa, 사전 라벨 재개를 검증한다.
