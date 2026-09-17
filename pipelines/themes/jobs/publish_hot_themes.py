"""백엔드 핫테마 발행 트리거.

일봉 적재가 끝난 뒤 백엔드 내부 API 를 호출해 핫테마 선정·Redis 발행을 맡긴다.
산출 로직은 백엔드 소유고, ETL 은 호출 시점만 보장한다.
"""

from __future__ import annotations

import logging
import os

import requests

_TIMEOUT_SECONDS = 10


def run() -> dict:
    base_url = os.environ.get("BACKEND_INTERNAL_URL", "").rstrip("/")
    token = os.environ.get("INTERNAL_API_TOKEN", "")
    if not base_url or not token:
        raise RuntimeError("BACKEND_INTERNAL_URL / INTERNAL_API_TOKEN 이 설정되지 않았습니다")

    response = requests.post(
        f"{base_url}/internal/hot-themes/publish",
        headers={"X-Internal-Token": token},
        timeout=_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    data = response.json()["data"]
    logging.info("핫테마 발행 결과: %s", data)
    return data
