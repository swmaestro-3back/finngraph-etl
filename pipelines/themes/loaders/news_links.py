from __future__ import annotations

from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

LINK_NEWS_THEMES_SQL = text(
    """
    INSERT INTO news_themes (news_id, theme_id)
    SELECT news_id, theme_id
      FROM (
        SELECT nr.news_id, ts.theme_id
          FROM news_relations AS nr
          JOIN stocks AS s ON s.ticker = nr.subject_code AND s.is_active
          JOIN theme_stocks AS ts ON ts.stock_id = s.id
         WHERE nr.subject_code IS NOT NULL
           AND nr.extracted_at >= now() - make_interval(hours => :window_hours)
         UNION
        SELECT nr.news_id, ts.theme_id
          FROM news_relations AS nr
          JOIN stocks AS s ON s.ticker = nr.object_code AND s.is_active
          JOIN theme_stocks AS ts ON ts.stock_id = s.id
         WHERE nr.object_code IS NOT NULL
           AND nr.extracted_at >= now() - make_interval(hours => :window_hours)
      ) AS matched
    ON CONFLICT (news_id, theme_id) DO NOTHING;
    """
)


def link_news_to_themes(window_hours: int = 24) -> int:

    with session_scope() as session:
        result = session.execute(LINK_NEWS_THEMES_SQL, {"window_hours": window_hours})
        linked = result.rowcount or 0

    logger.info("news_themes 연결: 신규 %d건 (최근 %d시간)", linked, window_hours)

    return linked
