from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]

ENV_FILES = (ROOT_DIR / ".env", ROOT_DIR / ".env.news")

# 쉼표 구분 실수 목록. NoDecode 가 없으면 pydantic-settings 가 JSON 으로 해석해 "1,1,1,1" 을
# 거부한다.
Weights = Annotated[tuple[float, ...], NoDecode]


class NewsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILES, env_file_encoding="utf-8", extra="ignore")

    client_id: SecretStr = Field(default=SecretStr(""), validation_alias="CLIENT_ID")
    client_secret: SecretStr = Field(default=SecretStr(""), validation_alias="CLIENT_SECRET")
    api_base_url: str = Field(default="", validation_alias="API_BASE_URL")

    search_display: int = Field(default=100, validation_alias="SEARCH_DISPLAY")
    # 최신순. 테마 last_searched_at 을 워터마크로 삼아 페이지를 멈추려면 날짜 순이어야 한다.
    search_sort: str = Field(default="date", validation_alias="SEARCH_SORT")
    # 종목명으로 실제 검색어를 만드는 서식 목록. 기업마다 서식 수만큼 검색한다.
    # 공급·계약 기사와 종목에 영향을 주는 수혜·호재·악재·특징주 기사를 고른다.
    # env 는 JSON 배열로 준다 (예: ["{name},공급","{name},특징주"]).
    search_query_templates: list[str] = Field(
        default=[
            "{name},공급",
            "{name},계약",
            "{name},수혜",
            "{name},호재",
            "{name},악재",
            "{name},특징주",
        ],
        validation_alias="NEWS_SEARCH_QUERY_TEMPLATES",
    )
    # 종목당 페이지 상한. 워터마크보다 오래된 기사가 나오면 그 전에 멈춘다.
    search_max_pages: int = Field(default=5, validation_alias="NEWS_SEARCH_MAX_PAGES")
    # 워터마크가 없는 종목(첫 검색)은 이 일수까지만 거슬러 수집한다.
    search_lookback_days: int = Field(default=180, validation_alias="NEWS_SEARCH_LOOKBACK_DAYS")
    # search_history.last_searched_at 이 이 간격을 넘긴 기업만 이번 런의 검색 대상이다.
    search_interval_hours: int = Field(default=2, validation_alias="NEWS_SEARCH_INTERVAL_HOURS")
    hot_themes_redis_url: str = Field(
        default="redis://localhost:16379/0", validation_alias="HOT_THEMES_REDIS_URL"
    )

    # 클러스터 판정(transformers/clustering/online.py). 점수 Γ = Σ 서브벡터(title·body·all·entities)
    # 코사인×text_weights + Σ 시간 피처(last·mean·first)×time_weights. 편입 조건은 Γ > threshold.
    # 기본값은 텍스트 1×4 에 시간을 보조 신호(0.1×3)로 둔 추정치다 — 운영값은 .env 에 둔다.
    cluster_threshold: float = Field(default=1.0, validation_alias="NEWS_CLUSTER_THRESHOLD")
    # title, body, all, entities 순
    cluster_text_weights: Weights = Field(
        default=(1.0, 1.0, 1.0, 1.0), validation_alias="NEWS_CLUSTER_TEXT_WEIGHTS"
    )
    # last, mean, first 순
    cluster_time_weights: Weights = Field(
        default=(0.1, 0.1, 0.1), validation_alias="NEWS_CLUSTER_TIME_WEIGHTS"
    )
    # 시간 피처 가우시안 폭(시간). 논문 σ=72h. 바꾸면 cluster_candidate_days 도 3σ 이상으로 맞춘다
    cluster_time_sigma_hours: float = Field(
        default=72.0, validation_alias="NEWS_CLUSTER_TIME_SIGMA_HOURS"
    )
    # 후보 클러스터 조회·활성 컷(일): last_published_at >= 기사 발행 − 이 값. 점수에는 영향이 없는
    # 성능용 컷이다
    cluster_candidate_days: int = Field(default=10, validation_alias="NEWS_CLUSTER_CANDIDATE_DAYS")
    # 클러스터 수명(일): first_published_at >= 기사 발행 − 이 값 인 클러스터만 후보다. 같은 기업
    # 기사가 며칠에 한 번씩 나오면 last 가 계속 밀려 클러스터가 몇 달을 사는 체인을 끊는다
    cluster_max_span_days: int = Field(default=10, validation_alias="NEWS_CLUSTER_MAX_SPAN_DAYS")
    # IDF 사전 코퍼스 기간(일). rebuild_idf 가 이 기간의 term_weights 있는 뉴스 전체를 집계한다
    cluster_idf_days: int = Field(default=90, validation_alias="NEWS_CLUSTER_IDF_DAYS")
    # 멤버 기사가 이 수에 이르면 대표 기사를 정한다(승격).
    cluster_promote_size: int = Field(default=3, validation_alias="NEWS_CLUSTER_PROMOTE_SIZE")
    # 승격을 다시 시도하는 범위(일). updated_at 이 이 안인 클러스터만 본다.
    cluster_promote_retry_days: int = Field(
        default=3, validation_alias="NEWS_CLUSTER_PROMOTE_RETRY_DAYS"
    )
    # 대표 후보 본문의 글자 수 하한. 이보다 긴 본문이 하나라도 있으면 짧은 본문은 대표가 되지
    # 않는다.
    cluster_representative_min_chars: int = Field(
        default=200, validation_alias="NEWS_CLUSTER_REPRESENTATIVE_MIN_CHARS"
    )
    cluster_keyword_count: int = Field(default=6, validation_alias="NEWS_CLUSTER_KEYWORD_COUNT")
    # 클러스터 이름 길이 상한. 시스템 프롬프트에 주입되고 검증에도 같은 값을 쓴다.
    cluster_title_max_chars: int = Field(
        default=25, validation_alias="NEWS_CLUSTER_TITLE_MAX_CHARS"
    )
    # 제목 생성에 넣는 멤버 제목 수(발행 시각순 앞에서부터).
    cluster_title_headlines: int = Field(
        default=10, validation_alias="NEWS_CLUSTER_TITLE_HEADLINES"
    )

    # 이슈 타임라인 연결(jobs/link_issues.py). 아래 임계값은 dev 데이터로 고른 초기값이다.
    # 주요 기업이 겹치는 앞선 이슈에 이을 때 쓰는 코사인 하한이다. 0.6 처럼 높이면 거의 같은 사건의
    # 중복만 잇고 실제 후속 이슈를 놓친다.
    issue_link_threshold: float = Field(default=0.45, validation_alias="NEWS_ISSUE_LINK_THRESHOLD")
    # 기업이 없는 이슈(거시 지표 등)끼리 이을 때 쓰는 코사인 하한이다. 기업 겹침 조건으로 거를 수
    # 없어서 더 엄격하게 잡는다.
    issue_link_no_company_threshold: float = Field(
        default=0.75, validation_alias="NEWS_ISSUE_LINK_NO_COMPANY_THRESHOLD"
    )
    # 연결 대상은 지금부터, 부모 후보는 대상의 first_published_at 부터 거슬러 이 일수 안에서 고른다.
    issue_link_lookback_days: int = Field(
        default=90, validation_alias="NEWS_ISSUE_LINK_LOOKBACK_DAYS"
    )
    issue_link_max_per_run: int = Field(default=200, validation_alias="NEWS_ISSUE_LINK_MAX_PER_RUN")
    # 연결 판정 방식이다. vote 는 LLM 투표(제안자 pair·rank, 확인자 judge·check)로 잇고 주가 반응
    # 이슈를 뺀다(transformers/issue_link_vote). cosine 은 주요 기업 겹침과 코사인 임계값만 보는
    # 이전 규칙이며, LLM 을 부르지 않으므로 문제가 생겼을 때 되돌리는 용도로 둔다. 투표 판정 모델은
    # BEDROCK_ISSUE_LINK_*_MODEL 로 정한다.
    issue_link_method: Literal["vote", "cosine"] = Field(
        default="vote", validation_alias="NEWS_ISSUE_LINK_METHOD"
    )
    # 투표 판정이 실행 한 번에 새로 부르는 LLM 호출 상한이다(캐시 적중은 세지 않는다). 상한에
    # 닿으면 남은 대상은 다음 실행으로 미룬다.
    issue_link_llm_max_calls_per_run: int = Field(
        default=1500, validation_alias="NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_RUN"
    )
    # 대상 이슈 하나가 실행 한 번에 새로 부르는 LLM 호출 상한이다. 상한에 닿으면 그 이슈만 미룬다.
    # 받은 응답은 캐시에 남으므로 다음 실행이 이어서 진행한다.
    issue_link_llm_max_calls_per_issue: int = Field(
        default=150, validation_alias="NEWS_ISSUE_LINK_LLM_MAX_CALLS_PER_ISSUE"
    )
    # 부모와의 관계를 같은 사건(same_event)으로 보는 기준이다. 첫 기사 시각 차가 이 시간 이내이거나,
    # 코사인이 issue_same_event_score 이상이면서 시각 차가 issue_same_event_score_max_gap_hours
    # 이내이면 같은 사건이 클러스터 둘로 나뉜 것으로 보고, 아니면 후속(follow_up)으로 본다.
    issue_same_event_max_gap_hours: float = Field(
        default=24, validation_alias="NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS"
    )
    issue_same_event_score: float = Field(
        default=0.75, validation_alias="NEWS_ISSUE_SAME_EVENT_SCORE"
    )
    # 코사인만으로 같은 사건이라고 보는 간격 상한(시간, 7일). 클러스터 수명
    # (NEWS_CLUSTER_MAX_SPAN_DAYS, 10일) 안의 값이다. 한 사건이 클러스터 둘로 나뉘는 일은 그 기간
    # 안에서 일어나지만, 시리즈 지표의 다음 회차(7월 → 8월 건설지출)는 코사인이 높아도 몇 주 떨어져
    # 있다.
    # 상한이 없으면 기업 없는 연결은 하한(0.75)이 점수 기준과 같아서 전부 같은 사건으로 합쳐진다.
    issue_same_event_score_max_gap_hours: float = Field(
        default=168, validation_alias="NEWS_ISSUE_SAME_EVENT_SCORE_MAX_GAP_HOURS"
    )
    # 먼저 시작한 이슈가 늦게 판정되면(승격·요약이 늦을 때), 그보다 뒤에 시작해 이미 판정된 이슈는
    # 그 이슈를 부모로 보지 못한 상태로 남는다. 그래서 실행마다 이번 대상 중 가장 먼저 시작한
    # 이슈보다 뒤에 시작해 이미 판정된 이슈를 다시 판정하되, 지금부터 이 시간 안에 시작한 이슈만
    # 고른다. 0 이면 다시 판정하지 않는다.
    issue_relink_window_hours: float = Field(
        default=72, validation_alias="NEWS_ISSUE_RELINK_WINDOW_HOURS"
    )
    # 이슈 연결에 쓰는 임베딩 모델이다. 테마용 BEDROCK_EMBEDDING_MODEL 은 질의 측(kg-api)과 함께
    # 바뀌므로 따로 둔다. 이 값을 바꾸면 기존 news_clusters.embedding 과 섞이지 않게
    # 백필 DAG 를 reset=true 로 실행해 임베딩을 다시 만든다.
    issue_embedding_model: str = Field(
        default="amazon.titan-embed-text-v2:0", validation_alias="NEWS_ISSUE_EMBEDDING_MODEL"
    )

    anchor_host: str = Field(default="", validation_alias="ANCHOR_HOST")

    request_delay: float = Field(default=1.0, validation_alias="REQUEST_DELAY")
    # 본문 크롤링 동시 요청 수. 대상이 여러 언론사 페이지라 API 제한은 없고,
    # 이 값이 유일한 상한이다.
    news_body_fetch_workers: int = Field(default=8, validation_alias="NEWS_BODY_FETCH_WORKERS")

    # 본문에만 나온 기업 후보(표기)가 이 수를 넘는 기사는 여러 종목을 모은 기사로 보고 저장하지
    # 않는다. 엔티티 LLM 도 부르지 않는다.
    news_body_candidate_max: int = Field(default=15, validation_alias="NEWS_BODY_CANDIDATE_MAX")

    news_llm_body_limit: int = Field(default=12000, validation_alias="NEWS_LLM_BODY_LIMIT")
    news_llm_max_tokens: int = Field(default=1024, validation_alias="NEWS_LLM_MAX_TOKENS")
    news_llm_max_concurrency: int = Field(default=8, validation_alias="NEWS_LLM_MAX_CONCURRENCY")
    # 관련성 필터가 한 번의 LLM 호출에 넣는 기사 수. 시스템 프롬프트 반복과 요청 수를 줄인다.
    news_llm_batch_size: int = Field(default=10, validation_alias="NEWS_LLM_BATCH_SIZE")

    @field_validator("cluster_text_weights", "cluster_time_weights", mode="before")
    @classmethod
    def _split_weights(cls, value):
        if isinstance(value, str):
            return tuple(float(part) for part in value.split(",") if part.strip())
        return value

    @field_validator("cluster_text_weights")
    @classmethod
    def _four_text_weights(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        if len(value) != 4:
            raise ValueError("NEWS_CLUSTER_TEXT_WEIGHTS 는 title,body,all,entities 4개여야 한다")
        return value

    @field_validator("cluster_time_weights")
    @classmethod
    def _three_time_weights(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        if len(value) != 3:
            raise ValueError("NEWS_CLUSTER_TIME_WEIGHTS 는 last,mean,first 3개여야 한다")
        return value

    @field_validator("anchor_host", mode="after")
    @classmethod
    def _normalize_host(cls, value: str) -> str:
        return value.strip().lower()


@lru_cache
def get_news_settings() -> NewsSettings:
    return NewsSettings()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
