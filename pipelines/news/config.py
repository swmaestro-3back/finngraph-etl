import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_BASE_URL = os.getenv("API_BASE_URL")
ANCHOR_HOST = os.getenv("ANCHOR_HOST", "").strip().lower()
ANCHOR_URL_TEMPLATE = os.getenv(
    "ANCHOR_URL_TEMPLATE",
    ""
).strip()


def load_anchor_categories() -> dict[int, str]:
    raw_categories = os.getenv("ANCHOR_CATEGORIES", "").strip()

    if not raw_categories:
        return {}

    try:
        parsed_categories = json.loads(raw_categories)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "ANCHOR_CATEGORIES 환경 변수는 올바른 JSON이어야 합니다."
        ) from e

    if not isinstance(parsed_categories, dict):
        raise RuntimeError(
            "ANCHOR_CATEGORIES 환경 변수는 JSON 객체여야 합니다."
        )

    try:
        return {
            int(category_id): str(category_name)
            for category_id, category_name in parsed_categories.items()
        }
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            "ANCHOR_CATEGORIES의 카테고리 ID는 정수여야 합니다."
        ) from e


ANCHOR_CATEGORIES = load_anchor_categories()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")
OLLAMA_EMBEDDING_MODEL = os.getenv("OLLAMA_EMBEDDING_MODEL")
OLLAMA_CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL")

VLLM_BASE_URL = os.getenv("VLLM_BASE_URL")
VLLM_CHAT_MODEL = os.getenv("VLLM_CHAT_MODEL")

MAX_TOTAL_COLLECTED_ITEMS = 80
OFFICIAL_SOURCE_THRESHOLD = 0
REQUEST_DELAY = 0.5

## 필터 관련 상수
ENABLE_MATERIAL_EVENT_FILTER = True
ENABLE_LLM_MATERIAL_EVENT_FILTER = True
MATERIAL_EVENT_FILTER_PROVIDER = os.getenv("MATERIAL_EVENT_FILTER_PROVIDER")
MATERIAL_EVENT_FILTER_BODY_LIMIT = int(os.getenv("MATERIAL_EVENT_FILTER_BODY_LIMIT"))
MATERIAL_EVENT_FILTER_MAX_TOKENS = int(os.getenv("MATERIAL_EVENT_FILTER_MAX_TOKENS"))
MATERIAL_EVENT_FILTER_MAX_WORKERS = int(os.getenv("MATERIAL_EVENT_FILTER_MAX_WORKERS"))
MATERIAL_EVENT_FILTER_BATCH_SIZE = int(os.getenv("MATERIAL_EVENT_FILTER_BATCH_SIZE"))
MATERIAL_EVENT_FILTER_FAIL_OPEN = (
    os.getenv("MATERIAL_EVENT_FILTER_FAIL_OPEN", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
