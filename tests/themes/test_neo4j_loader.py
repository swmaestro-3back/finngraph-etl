from __future__ import annotations

from pipelines.themes.loaders.neo4j import build_theme_batch
from pipelines.themes.models import Company, Theme


def _theme(name: str, source: str = "naver") -> Theme:
    return Theme(
        name=name,
        source=source,
        source_theme_id=999,
        description=f"{name} 설명",
        companies=[Company(name="삼성전자", ticker="005930", reason="이유")],
    )


def test_batch_uses_postgres_id_not_source_theme_id() -> None:
    batch, skipped = build_theme_batch([_theme("철강")], {"철강": 42})

    assert skipped == []
    assert len(batch) == 1
    assert batch[0]["theme_id"] == 42
    assert "source_theme_id" not in batch[0]
    assert batch[0]["companies"] == [{"ticker": "005930", "reason": "이유"}]


def test_theme_without_postgres_id_is_skipped() -> None:
    batch, skipped = build_theme_batch([_theme("철강"), _theme("반도체")], {"철강": 1})

    assert [row["name"] for row in batch] == ["철강"]
    assert skipped == ["반도체"]
