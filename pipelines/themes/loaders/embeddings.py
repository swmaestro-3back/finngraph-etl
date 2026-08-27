"""임베딩 대상 선정 — 텍스트 조립·해시·스테일 판정의 순수 함수."""

from __future__ import annotations

import hashlib
from typing import Any

EMBEDDING_DIM = 1024


def theme_text(name: str, description: str | None) -> str:
    """테마 임베딩 입력 텍스트. 기존 pg 시절 표현(name || '\\n' || coalesce(desc,''))과 같다."""

    return f"{name}\n{description or ''}"


def text_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def stale_targets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """임베딩이 없거나 텍스트가 바뀐 행만 남긴다.

    각 행은 text(임베딩 입력)·stored_hash(저장된 해시)·has_embedding(벡터 존재)을
    담아야 하며, 스테일 행에는 기록에 되쓸 text_hash 를 붙여 반환한다.
    """

    stale: list[dict[str, Any]] = []
    for row in rows:
        current_hash = text_hash(row["text"])
        if row["has_embedding"] and row["stored_hash"] == current_hash:
            continue
        stale.append({**row, "text_hash": current_hash})
    return stale
