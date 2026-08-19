"""task: link_news — 최근 뉴스-관계를 테마에 연결한다.

`themes_refresh`가 아니라 `etl://news/relations` Asset으로 트리거되는 별개 DAG의
유일한 태스크다. 트리거가 다르므로 테마 갱신 DAG와 합칠 수 없다.
"""

from __future__ import annotations

from pipelines.common.logging import get_logger
from pipelines.themes.loaders.postgres import link_news_to_themes

logger = get_logger(__name__)

DEFAULT_WINDOW_HOURS = 24


def run(window_hours: int | None = None) -> None:
    window = window_hours or DEFAULT_WINDOW_HOURS
    linked = link_news_to_themes(window_hours=window)

    logger.info("뉴스-테마 연결 완료: 최근 %d시간, 신규 %d건", window, linked)
