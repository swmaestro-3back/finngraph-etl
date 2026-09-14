"""collect_articles 에 넘길 테마 선정.

명시된 theme_ids 가 있으면 그대로 쓰고(수동 트리거), 없으면 최신 일봉 기준 급등락 테마를
고른다.
"""

from __future__ import annotations

import logging

from pipelines.common.utils.time import now_kst
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.theme_changes import fetch_theme_changes
from pipelines.news.transformers.theme_momentum import select_momentum_themes


def run(theme_ids: list[int] | None = None) -> list[int]:
    if theme_ids:
        return [int(theme_id) for theme_id in theme_ids]

    changes = fetch_theme_changes(as_of=now_kst().date())
    selected = select_momentum_themes(changes, get_news_settings().theme_count)

    by_id = {c.theme_id: c for c in changes}
    logging.info(
        "급등락 테마 %d개 선정 (기준일 %s, 후보 %d개): %s",
        len(selected),
        changes[0].trade_date if changes else None,
        len(changes),
        [f"{by_id[i].name} {by_id[i].change:+.1f}%" for i in selected],
    )

    return selected
