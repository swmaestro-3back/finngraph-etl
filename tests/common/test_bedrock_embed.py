"""embed_texts 모델 선택 — model 을 안 주면 BEDROCK_EMBEDDING_MODEL, 주면 그 모델이다.

Bedrock 클라이언트는 가짜로 갈아끼운다. 요청 본문과 modelId 만 본다.
"""

from __future__ import annotations

import io
import json

import pytest

from pipelines.common import config
from pipelines.common.clients import bedrock


class FakeClient:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def invoke_model(self, modelId, body):  # noqa: N803 — boto3 인자 이름 그대로
        payload = json.loads(body)
        self.calls.append((modelId, payload))
        vector = [float(len(payload["inputText"]))] * payload["dimensions"]
        return {"body": io.BytesIO(json.dumps({"embedding": vector}).encode())}


@pytest.fixture
def client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setenv("BEDROCK_EMBEDDING_MODEL", "theme-model")
    monkeypatch.setenv("BEDROCK_EMBEDDING_MAX_WORKERS", "4")
    config.get_settings.cache_clear()
    monkeypatch.setattr(bedrock, "get_bedrock_client", lambda *args, **kwargs: fake)
    yield fake
    config.get_settings.cache_clear()


def test_embed_texts_defaults_to_theme_model(client):
    vectors = bedrock.embed_texts(["가", "가나"], dim=2)

    assert vectors == [[1.0, 1.0], [2.0, 2.0]]
    assert {model for model, _ in client.calls} == {"theme-model"}
    assert all(payload["normalize"] is True for _, payload in client.calls)


def test_embed_texts_uses_given_model(client):
    vectors = bedrock.embed_texts(["가나다"], dim=3, model="issue-model")

    assert vectors == [[3.0, 3.0, 3.0]]
    assert client.calls == [
        ("issue-model", {"inputText": "가나다", "dimensions": 3, "normalize": True})
    ]
