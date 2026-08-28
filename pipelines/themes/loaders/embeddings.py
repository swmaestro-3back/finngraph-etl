"""임베딩 입력 텍스트 조립의 순수 함수."""

from __future__ import annotations

EMBEDDING_DIM = 1024


def theme_text(name: str, description: str | None) -> str:
    """테마 임베딩 입력 텍스트. 기존 pg 시절 표현(name || '\\n' || coalesce(desc,''))과 같다."""

    return f"{name}\n{description or ''}"
