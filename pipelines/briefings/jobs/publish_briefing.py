from __future__ import annotations

import logging
import os

import requests

_TIMEOUT_SECONDS = 300


def run(date: str | None = None, force: bool = False) -> dict:
    base_url = os.environ.get("BACKEND_INTERNAL_URL", "").rstrip("/")
    token = os.environ.get("INTERNAL_API_TOKEN", "")
    if not base_url or not token:
        raise RuntimeError("BACKEND_INTERNAL_URL / INTERNAL_API_TOKEN 이 설정되지 않았습니다")

    params: dict[str, str] = {}
    if date:
        params["date"] = date
    if force:
        params["force"] = "true"

    response = requests.post(
        url=f"{base_url}/internal/briefings/generate",
        headers={"X-Internal-Token": token},
        params=params,
        timeout=_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    data = response.json()["data"]
    logging.info("브리핑 생성 결과: %s", data)
    return data
