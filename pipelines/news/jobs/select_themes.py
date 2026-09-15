from __future__ import annotations

import logging

from pipelines.common.utils.time import now_kst
from pipelines.news.config import get_news_settings
from pipelines.news.repositories.hot_themes import fetch_hot_themes
from pipelines.news.repositories.theme_changes import (
    fetch_latest_trade_date,
    fetch_theme_changes,
)
from pipelines.news.transformers.theme_momentum import select_momentum_themes


def run(theme_ids: list[int] | None = None) -> list[int]:
    if theme_ids:
        return [int(theme_id) for theme_id in theme_ids]

    as_of = now_kst().date()

    hot = fetch_hot_themes()
    if hot is not None:
        latest = fetch_latest_trade_date(as_of=as_of)
        if hot.trade_date is not None and hot.trade_date == latest:
            logging.info(
                "백엔드 핫테마 %d개 사용 (기준일 %s): %s",
                len(hot.theme_ids),
                hot.trade_date,
                hot.theme_ids,
            )
            return hot.theme_ids
        logging.warning(
            "핫테마 기준일 불일치 (페이로드 %s, DB %s) — 자체 선정으로 폴백한다",
            hot.trade_date,
            latest,
        )

    changes = fetch_theme_changes(as_of=as_of)
    selected = select_momentum_themes(changes, get_news_settings().theme_count)

    by_id = {c.theme_id: c for c in changes}
    logging.info(
        "급등락 테마 %d개 자체 선정 (기준일 %s, 후보 %d개): %s",
        len(selected),
        changes[0].trade_date if changes else None,
        len(changes),
        [f"{by_id[i].name} {by_id[i].change:+.1f}%" for i in selected],
    )

    return selected
