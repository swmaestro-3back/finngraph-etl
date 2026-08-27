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


def ensure_bedrock_token() -> None:
    """Settings의 bearer 토큰을 env로 주입한다.

    boto3 기반 클라이언트뿐 아니라 langchain-aws(ChatBedrockConverse)도 env의
    AWS_BEARER_TOKEN_BEDROCK을 읽으므로, Bedrock 클라이언트 생성 전에 호출한다.
    """
    token = get_settings().aws_bearer_token_bedrock
    if token:
        os.environ.setdefault("AWS_BEARER_TOKEN_BEDROCK", token)


@lru_cache
def get_bedrock_client(region: str, timeout: int) -> Any:
    import boto3
    from botocore.config import Config

    ensure_bedrock_token()

    return boto3.client(
        "bedrock-runtime",
        region_name=region or None,
        config=Config(
            read_timeout=timeout,
            connect_timeout=timeout,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


# Titan invoke_model 은 요청당 텍스트 1건에 건당 ~0.5초라 순차로는 전량 백필이
# 시간 단위로 걸린다. boto3 클라이언트는 스레드 안전하므로 스레드로 병렬화하되,
# 워커 수는 Titan 의 분당 요청 쿼터 안쪽으로 잡는다.
EMBED_MAX_WORKERS = 8


def embed_texts(texts: list[str], dim: int) -> list[list[float]]:
    """Titan Embed v2 로 텍스트 목록을 임베딩한다. 순서는 입력 순서와 같다.

    normalize=True 지만 조회가 코사인(<=>)이라 결과에는 영향이 없다.
    """
    import json
    from concurrent.futures import ThreadPoolExecutor

    settings = get_settings()
    client = get_bedrock_client(settings.bedrock_region, settings.bedrock_request_timeout)

    def embed_one(text: str) -> list[float]:
        response = client.invoke_model(
            modelId=settings.bedrock_embedding_model,
            body=json.dumps({"inputText": text, "dimensions": dim, "normalize": True}),
        )
        return json.loads(response["body"].read())["embedding"]

    if len(texts) <= 1:
        return [embed_one(text) for text in texts]

    with ThreadPoolExecutor(max_workers=EMBED_MAX_WORKERS) as pool:
        return list(pool.map(embed_one, texts))


def extract_bedrock_text(response_data: dict[str, Any]) -> str:
    content = response_data.get("output", {}).get("message", {}).get("content", [])

    for block in content:
        if isinstance(block, dict) and "text" in block:
            return block["text"]

    return ""
