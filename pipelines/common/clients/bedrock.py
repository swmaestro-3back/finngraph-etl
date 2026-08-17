"""AWS Bedrock 공용 헬퍼.

여러 파이프라인이 같은 Bedrock 런타임 클라이언트·Converse 응답 파싱을 쓰도록
common에 둔다. 접속 설정(BEDROCK_REGION/BEDROCK_CHAT_MODEL/BEDROCK_REQUEST_TIMEOUT)은
`pipelines.common.config.Settings`가 담당한다.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from pipelines.common.config import get_settings


@lru_cache
def get_bedrock_client(region: str, timeout: int) -> Any:
    import boto3
    from botocore.config import Config

    token = get_settings().aws_bearer_token_bedrock
    if token:
        os.environ.setdefault("AWS_BEARER_TOKEN_BEDROCK", token)

    return boto3.client(
        "bedrock-runtime",
        region_name=region or None,
        config=Config(
            read_timeout=timeout,
            connect_timeout=timeout,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


def extract_bedrock_text(response_data: dict[str, Any]) -> str:
    content = response_data.get("output", {}).get("message", {}).get("content", [])

    for block in content:
        if isinstance(block, dict) and "text" in block:
            return block["text"]

    return ""
