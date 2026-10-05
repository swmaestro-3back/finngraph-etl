"""역할별 Bedrock 채팅 모델 설정 — 비어 있으면 공통 BEDROCK_CHAT_MODEL 로 떨어진다."""

from __future__ import annotations

import pytest

from pipelines.common.config import Settings

ROLES = {
    "relevance": "BEDROCK_RELEVANCE_MODEL",
    "entity": "BEDROCK_ENTITY_MODEL",
    "cluster_title": "BEDROCK_CLUSTER_TITLE_MODEL",
    "summary": "BEDROCK_SUMMARY_MODEL",
    "triples": "BEDROCK_TRIPLES_MODEL",
}


@pytest.fixture
def env(monkeypatch):
    for name in ROLES.values():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("BEDROCK_CHAT_MODEL", "base-model")
    return monkeypatch


@pytest.mark.parametrize("role", ROLES)
def test_role_model_falls_back_to_chat_model(env, role):
    assert Settings(_env_file=None).chat_model(role) == "base-model"


@pytest.mark.parametrize(("role", "variable"), ROLES.items())
def test_role_model_overrides_chat_model(env, role, variable):
    env.setenv(variable, "light-model")

    settings = Settings(_env_file=None)

    assert settings.chat_model(role) == "light-model"
    # 다른 역할은 공통 모델 그대로다
    assert {settings.chat_model(other) for other in ROLES if other != role} == {"base-model"}


def test_unknown_role_is_rejected(env):
    with pytest.raises(ValueError):
        Settings(_env_file=None).chat_model("titles")
