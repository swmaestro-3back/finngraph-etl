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
def get_bedrock_client(region: str, timeout: int, max_pool_connections: int = 10) -> Any:
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
            # 기본 풀은 10 이라 스레드가 그보다 많으면 연결을 버렸다 다시 맺는다.
            max_pool_connections=max_pool_connections,
        ),
    )


# Titan invoke_model 은 요청당 텍스트 1건만 받아(다건 배치 API 없음) 건당 ~0.5초가 걸린다.
# boto3 클라이언트는 스레드 안전하므로 스레드로 병렬화하되, 워커 수는 Titan 의 분당 요청
# 쿼터 안쪽으로 잡는다(BEDROCK_EMBEDDING_MAX_WORKERS).


def embed_texts(texts: list[str], dim: int, model: str | None = None) -> list[list[float]]:
    """Titan Embed v2 로 텍스트 목록을 임베딩한다. 순서는 입력 순서와 같다.

    normalize=True 지만 조회가 코사인이라 결과에는 영향이 없다. model 을 주지 않으면
    BEDROCK_EMBEDDING_MODEL 을 쓴다. 이 값은 테마 임베딩용이라 질의 측(kg-api)과 함께 바뀌므로,
    다른 용도의 벡터는 자기 모델 설정을 넘겨서 그 변경에 영향받지 않게 한다.
    """
    import json
    from concurrent.futures import ThreadPoolExecutor

    settings = get_settings()
    model_id = model or settings.bedrock_embedding_model
    max_workers = max(1, settings.bedrock_embedding_max_workers)
    client = get_bedrock_client(
        settings.bedrock_region,
        settings.bedrock_request_timeout,
        max_pool_connections=max_workers,
    )

    def embed_one(text: str) -> list[float]:
        response = client.invoke_model(
            modelId=model_id,
            body=json.dumps({"inputText": text, "dimensions": dim, "normalize": True}),
        )
        return json.loads(response["body"].read())["embedding"]

    if len(texts) <= 1:
        return [embed_one(text) for text in texts]

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(embed_one, texts))


def extract_bedrock_text(response_data: dict[str, Any]) -> str:
    content = response_data.get("output", {}).get("message", {}).get("content", [])

    for block in content:
        if isinstance(block, dict) and "text" in block:
            return block["text"]

    return ""
