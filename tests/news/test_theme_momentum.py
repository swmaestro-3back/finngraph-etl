"""급등락 테마 선정 단위 테스트 (DB 불필요)."""

from __future__ import annotations

from datetime import date

from pipelines.news.repositories.theme_changes import ThemeChange
from pipelines.news.transformers.theme_momentum import select_momentum_themes

D = date(2026, 9, 11)


def _change(theme_id: int, change: float) -> ThemeChange:
    return ThemeChange(theme_id, f"테마{theme_id}", D, change, stock_count=5)


def test_picks_top_ups_then_top_downs():
    changes = [
        _change(1, 3.0),
        _change(2, -1.0),
        _change(3, 7.5),
        _change(4, -4.2),
        _change(5, 0.5),
    ]

    assert select_momentum_themes(changes, count=4) == [3, 1, 4, 2]


def test_odd_count_gives_extra_slot_to_ups():
    changes = [_change(1, 1.0), _change(2, 2.0), _change(3, -1.0), _change(4, -2.0)]

    assert select_momentum_themes(changes, count=3) == [2, 1, 4]


def test_zero_change_is_neither_up_nor_down():
    changes = [_change(1, 0.0), _change(2, 1.0)]

    assert select_momentum_themes(changes, count=40) == [2]


def test_ties_break_by_theme_id_for_determinism():
    changes = [_change(9, 2.0), _change(3, 2.0), _change(5, -2.0), _change(1, -2.0)]

    assert select_momentum_themes(changes, count=2) == [3, 1]


def test_empty():
    assert select_momentum_themes([], count=40) == []
