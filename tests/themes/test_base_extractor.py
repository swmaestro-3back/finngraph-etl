"""BaseExtractor 공통 동작 단위 테스트 (네트워크 불필요)."""

from __future__ import annotations

from pipelines.themes.extractors import JudalExtractor
from pipelines.themes.models import Company, Theme


def _theme(name: str, stock_count: int) -> Theme:
    return Theme(
        name=name,
        source="judal",
        companies=[Company(name=f"종목{i}", ticker=f"{i:06d}") for i in range(stock_count)],
    )


def test_drop_small_themes_excludes_themes_with_two_or_fewer_stocks() -> None:
    themes = [_theme("빈 테마", 0), _theme("CCTV", 2), _theme("반도체", 3), _theme("2차전지", 10)]

    kept = JudalExtractor().drop_small_themes(themes)

    assert [t.name for t in kept] == ["반도체", "2차전지"]
