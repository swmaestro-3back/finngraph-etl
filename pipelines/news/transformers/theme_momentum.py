"""급등락 테마 선정 — 순수 함수.

프론트 selectTreemapThemes 와 같은 규칙이다: 상승 테마를 등락률 내림차순으로 ceil(count/2)개,
하락 테마를 등락률 오름차순으로 floor(count/2)개. 등락률 0 은 어느 쪽에도 들지 않는다.
"""

from __future__ import annotations

import math

from pipelines.news.repositories.theme_changes import ThemeChange


def select_momentum_themes(changes: list[ThemeChange], count: int) -> list[int]:
    """상승 상위 + 하락 상위 테마 ID. 상승분이 먼저 온다."""

    ups = sorted((c for c in changes if c.change > 0), key=lambda c: (-c.change, c.theme_id))
    downs = sorted((c for c in changes if c.change < 0), key=lambda c: (c.change, c.theme_id))
    selected = ups[: math.ceil(count / 2)] + downs[: math.floor(count / 2)]

    return [c.theme_id for c in selected]
