from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[2]

ENV_FILES = (ROOT_DIR / ".env", ROOT_DIR / ".env.news")


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

    cluster_threshold: float = Field(default=0.35, validation_alias="NEWS_CLUSTER_THRESHOLD")
    cluster_description_weight: float = Field(
        default=0.4,
        validation_alias="NEWS_CLUSTER_DESCRIPTION_WEIGHT",
    )
    # 클러스터 판정에 쓰는 본문 리드 길이(자). 제목이 주 신호이고, 본문 앞 이만큼을
    # NEWS_CLUSTER_DESCRIPTION_WEIGHT 로 낮춰 더한다.
    cluster_lead_chars: int = Field(default=200, validation_alias="NEWS_CLUSTER_LEAD_CHARS")

    cluster_window_days: int = Field(default=7, validation_alias="NEWS_CLUSTER_WINDOW_DAYS")
    # 클러스터 첫 기사보다 먼저 발행된 기사를 받아 주는 여유(일). 상대방 기업을 나중 런에서
    # 검색하면 같은 사건의 조금 이른 기사가 뒤늦게 들어온다.
    cluster_backward_days: int = Field(default=1, validation_alias="NEWS_CLUSTER_BACKWARD_DAYS")
    # 전역 IDF 를 집계하는 범위(일). 이 기간에 수집된 후보 기사의 cluster_terms 만 센다.
    cluster_idf_days: int = Field(default=90, validation_alias="NEWS_CLUSTER_IDF_DAYS")
    # 후보 기사가 이 수에 이르면 대표 기사를 정한다(승격). 클러스터는 이 수까지만 후보를 받는다.
    cluster_promote_size: int = Field(default=3, validation_alias="NEWS_CLUSTER_PROMOTE_SIZE")
    # 승격을 다시 시도하는 범위(일). updated_at 이 이 안인 클러스터만 본다.
    cluster_promote_retry_days: int = Field(
        default=3, validation_alias="NEWS_CLUSTER_PROMOTE_RETRY_DAYS"
    )
    # 대표 후보 본문의 글자 수 하한. 이보다 긴 본문이 하나라도 있으면
    # 짧은 본문은 대표가 되지 않는다.
    cluster_representative_min_chars: int = Field(
        default=200, validation_alias="NEWS_CLUSTER_REPRESENTATIVE_MIN_CHARS"
    )

    cluster_keyword_count: int = Field(default=6, validation_alias="NEWS_CLUSTER_KEYWORD_COUNT")
    # 클러스터 이름 길이 상한. 시스템 프롬프트에 주입되고 검증에도 같은 값을 쓴다.
    cluster_title_max_chars: int = Field(
        default=25, validation_alias="NEWS_CLUSTER_TITLE_MAX_CHARS"
    )

    # 이슈 타임라인 연결(jobs/link_issues.py). 아래 임계값은 dev 데이터로 고른 초기값이다.
    # 스케줄 연결을 켜고 끄는 설정이다. 기존 클러스터 연결 백필
    # (news_backfill_issue_timeline DAG)이 끝난 뒤에 켠다. 먼저 켜면 lookback 안의 최근 이슈만
    # 보고 판정하므로, 그보다 오래된 이슈를 부모로 보지 못한 채 루트(부모가 없는 타임라인 첫
    # 이슈)로 남는다. 백필 DAG 는 이 값과 무관하게 돈다.
    issue_link_enabled: bool = Field(default=False, validation_alias="NEWS_ISSUE_LINK_ENABLED")
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
    # 부모와의 관계를 같은 사건(same_event)으로 보는 기준이다. 첫 기사 시각 차가 이 시간 이내이거나,
    # 코사인이 issue_same_event_score 이상이면서 시각 차가 issue_same_event_score_max_gap_hours
    # 이내이면 같은 사건이 클러스터 둘로 나뉜 것으로 보고, 아니면 후속(follow_up)으로 본다.
    issue_same_event_max_gap_hours: float = Field(
        default=24, validation_alias="NEWS_ISSUE_SAME_EVENT_MAX_GAP_HOURS"
    )
    issue_same_event_score: float = Field(
        default=0.75, validation_alias="NEWS_ISSUE_SAME_EVENT_SCORE"
    )
    # 코사인만으로 같은 사건이라고 보는 간격 상한(시간)이며, 클러스터 기간
    # (NEWS_CLUSTER_WINDOW_DAYS, 7일)과 같다. 한 사건이 클러스터 둘로 나뉘는 일은 그 기간 안에서
    # 일어나지만, 시리즈 지표의 다음 회차(7월 → 8월 건설지출)는 코사인이 높아도 몇 주 떨어져 있다.
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
    # --reset-links 로 임베딩을 다시 만든다.
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
