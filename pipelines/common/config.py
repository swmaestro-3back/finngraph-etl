from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    # .env 파일을 읽어오기 위한 설정
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="ignore")

    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    database_url: str = Field(
        default="postgresql+psycopg://threeback:12345678@localhost:15432/finngraph",
        validation_alias="DATABASE_URL",
    )

    kis_app_key: str = Field(default="", validation_alias="KIS_APP_KEY")
    kis_app_secret: str = Field(default="", validation_alias="KIS_APP_SECRET")
    kis_account_no: str = Field(default="", validation_alias="KIS_ACCOUNT_NO")
    kis_rate_limit_per_second: int = Field(default=15, validation_alias="KIS_RATE_LIMIT_PER_SECOND")
    kis_base_url: str = Field(
        default="https://openapi.koreainvestment.com:9443",
        validation_alias="KIS_BASE_URL",
    )
    # 접근토큰은 발급 자체에 분당 1회 제한이 있고 유효기간이 24시간이다. job마다 새로 발급하면
    # 곧바로 한도에 걸리므로 파일에 캐시해 프로세스 간에 재사용한다.
    kis_token_cache_path: str = Field(
        default="/tmp/etl-kis-token.json",
        validation_alias="KIS_TOKEN_CACHE_PATH",
    )

    # === OpenDART ===
    # 인증키는 일 20,000건 한도라 초당 값은 넉넉히 잡아도 무방하다. 병목은 호출 수가 아니라
    # 하루 한도이므로, batch_size로 회차당 처리량을 조절한다.
    # 같은 인증키를 두 이름으로 받는다. 별도 키로 두면 한쪽만 설정한 배포에서 다른 쪽
    # 파이프라인이 빈 키로 조용히 실패한다. 둘 다 있으면 DART_API_KEY 가 우선이다.
    dart_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("DART_API_KEY", "OPENDART_API_KEY"),
    )
    dart_base_url: str = Field(
        default="https://opendart.fss.or.kr/api",
        validation_alias="DART_BASE_URL",
    )
    dart_rate_limit_per_second: int = Field(
        default=10,
        validation_alias="DART_RATE_LIMIT_PER_SECOND",
    )
    # 개요·재무 모두 service_companies(361법인)로 좁혀져 있어 한 회차에 거의 다 돈다.
    dart_profile_batch_size: int = Field(default=200, validation_alias="DART_PROFILE_BATCH_SIZE")
    dart_financial_batch_size: int = Field(
        default=100,
        validation_alias="DART_FINANCIAL_BATCH_SIZE",
    )
    dart_financial_years: int = Field(default=3, validation_alias="DART_FINANCIAL_YEARS")

    # === disclosures: 단일판매ㆍ공급계약체결 공시 ===
    # 백필 소급 연수와 일별 갱신 시 되짚어볼 구간. 근거는 STOCK_DAILY_* 와 같다 — 짧으면
    # 장애로 빠진 날을 못 채우고, 길면 매일 그만큼 재조회한다. rcept_no UNIQUE 덕에 재조회
    # 비용은 목록 API 페이지 수뿐이라 겹쳐 잡아도 원문을 다시 받지는 않는다.
    disclosure_backfill_years: int = Field(
        default=3,
        validation_alias="DISCLOSURE_BACKFILL_YEARS",
    )
    disclosure_daily_lookback_days: int = Field(
        default=3,
        validation_alias="DISCLOSURE_DAILY_LOOKBACK_DAYS",
    )
    # 한 회차에 새로 조회할 원문 문서 수 상한. 인증키의 일 호출 한도를 지키는 안전판으로,
    # 3개년 백필(문서 ~1.1만 건)이 다른 DART job 과 하루 한도를 나눠 쓰게 한다.
    disclosure_fetch_batch_size: int = Field(
        default=8000,
        validation_alias="DISCLOSURE_FETCH_BATCH_SIZE",
    )

    stock_intraday_retention_days: int = Field(
        default=60,
        validation_alias="STOCK_INTRADAY_RETENTION_DAYS",
    )
    stock_intraday_target_delay_minutes: int = Field(
        default=5,
        validation_alias="STOCK_INTRADAY_TARGET_DELAY_MINUTES",
    )

    # 일봉 백필 소급 연수와, 일별 갱신 시 되짚어볼 구간. 갱신 구간이 짧으면 휴장·장애로 빠진
    # 날을 영영 못 채우고, 길면 매일 그만큼 재조회한다.
    stock_daily_backfill_years: int = Field(
        default=10,
        validation_alias="STOCK_DAILY_BACKFILL_YEARS",
    )
    stock_daily_lookback_days: int = Field(
        default=10,
        validation_alias="STOCK_DAILY_LOOKBACK_DAYS",
    )
    stock_period_lookback_days: int = Field(
        default=120,
        validation_alias="STOCK_PERIOD_LOOKBACK_DAYS",
    )

    bedrock_region: str = Field(default="", validation_alias="BEDROCK_REGION")
    bedrock_chat_model: str = Field(default="", validation_alias="BEDROCK_CHAT_MODEL")
    bedrock_request_timeout: int = Field(default=300, validation_alias="BEDROCK_REQUEST_TIMEOUT")
    aws_bearer_token_bedrock: str = Field(default="", validation_alias="AWS_BEARER_TOKEN_BEDROCK")

    neo4j_uri: str = Field(default="", validation_alias="NEO4J_URI")
    neo4j_username: str = Field(default="", validation_alias="NEO4J_USERNAME")
    neo4j_password: str = Field(default="", validation_alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="", validation_alias="NEO4J_DATABASE")

    dart_api_key: str = Field(default="", validation_alias="DART_API_KEY")
    dart_base_url: str = Field(
        default="https://opendart.fss.or.kr/api",
        validation_alias="DART_BASE_URL",
    )
    # OpenDART 공식 한도는 분당 1,000회다. 여유를 두고 초당 10회로 잡는다.
    dart_rate_limit_per_second: int = Field(
        default=10, validation_alias="DART_RATE_LIMIT_PER_SECOND"
    )
    company_financial_batch_size: int = Field(
        default=300,
        validation_alias="COMPANY_FINANCIAL_BATCH_SIZE",
    )
    dart_profile_batch_size: int = Field(default=200, validation_alias="DART_PROFILE_BATCH_SIZE")
    dart_financial_batch_size: int = Field(
        default=100,
        validation_alias="DART_FINANCIAL_BATCH_SIZE",
    )
    # 사업연도 몇 개를 받을지. 법인·연도당 CFS/OFS 2회라 늘리면 호출 수가 비례해 는다.
    dart_financial_years: int = Field(default=3, validation_alias="DART_FINANCIAL_YEARS")


@lru_cache
def get_settings() -> Settings:
    return Settings()
