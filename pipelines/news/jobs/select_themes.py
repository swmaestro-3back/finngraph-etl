from __future__ import annotations

import logging

from pipelines.common.utils.time import now_kst
from pipelines.news.repositories.hot_themes import HotThemesUnavailableError, fetch_hot_themes
from pipelines.news.repositories.trade_dates import fetch_latest_trade_date


def run(theme_ids: list[int] | None = None) -> list[int]:
    if theme_ids:
        return [int(theme_id) for theme_id in theme_ids]

    hot = fetch_hot_themes()
    latest = fetch_latest_trade_date(as_of=now_kst().date())
    if hot.trade_date is None or hot.trade_date != latest:
        raise HotThemesUnavailableError(
            f"핫테마 기준일 불일치 (페이로드 {hot.trade_date}, DB {latest})"
        )

    logging.info(
        "백엔드 핫테마 %d개 사용 (기준일 %s): %s",
        len(hot.theme_ids),
        hot.trade_date,
        hot.theme_ids,
    )
    return hot.theme_ids
