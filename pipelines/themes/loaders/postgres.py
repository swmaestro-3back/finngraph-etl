"""테마 Postgres 적재.

`load_themes`: 크롤링 스냅샷을 themes/theme_stocks에 정합 (themes_refresh, 일 1회)
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.loaders.tickers import fetch_active_stock_ids

logger = get_logger(__name__)

MIN_THEMES_SAFETY = 1


def _upsert_theme(session, name: str, description: str) -> int:
    row = session.execute(
        text(
            """
            INSERT INTO themes (name, description)
            VALUES (:name, :description)
            ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
            RETURNING id;
            """
        ),
        {"name": name, "description": description},
    ).fetchone()

    return row[0]


def _reconcile_theme_stocks(
    session, theme_id: int, companies: list[dict[str, Any]], stock_ids: dict[str, int]
) -> tuple[int, int]:
    """스냅샷 기준으로 theme_stocks를 정합한다. (연결 수, 미매칭 수) 반환."""

    linked_ids: list[int] = []
    unmatched = 0

    for company in companies:
        ticker = (company.get("ticker") or "").strip()
        stock_id = stock_ids.get(ticker)

        if not stock_id:
            unmatched += 1
            continue

        linked_ids.append(stock_id)
        session.execute(
            text(
                """
                INSERT INTO theme_stocks (theme_id, stock_id, reason)
                VALUES (:theme_id, :stock_id, :reason)
                ON CONFLICT (theme_id, stock_id) DO UPDATE SET reason = EXCLUDED.reason;
                """
            ),
            {"theme_id": theme_id, "stock_id": stock_id, "reason": company.get("reason")},
        )

    if linked_ids:
        session.execute(
            text(
                "DELETE FROM theme_stocks "
                "WHERE theme_id = :theme_id AND NOT (stock_id = ANY(:ids));"
            ),
            {"theme_id": theme_id, "ids": linked_ids},
        )
    else:
        # 빈 스냅샷은 "종목이 없어짐"과 "크롤 결측"을 구분할 수 없으므로 기존 편입을 보존한다.
        logger.warning("테마 편입 스냅샷이 비어 삭제를 건너뜀: theme_id=%s", theme_id)

    return len(linked_ids), unmatched


def load_themes(themes: list[dict[str, Any]]) -> dict[str, Any]:

    if len(themes) < MIN_THEMES_SAFETY:
        logger.error("테마 스냅샷이 비어 있어 적재를 중단합니다(정상 데이터 덮어쓰기 방지).")
        return {"themes": 0, "theme_stocks": 0, "unmatched": 0, "failed": 0, "skipped": True}

    theme_count = 0
    stock_count = 0
    unmatched_count = 0
    failed_count = 0

    with session_scope() as session:
        stock_ids = fetch_active_stock_ids(session)

        for theme in themes:
            name = (theme.get("name") or "").strip()

            if not name:
                continue

            try:
                with session.begin_nested():
                    theme_id = _upsert_theme(
                        session, name=name, description=theme.get("description", "") or ""
                    )
                    linked, unmatched = _reconcile_theme_stocks(
                        session, theme_id, theme.get("companies", []) or [], stock_ids
                    )

                theme_count += 1
                stock_count += linked
                unmatched_count += unmatched

            except Exception as e:
                failed_count += 1
                logger.error("테마 적재 실패: name=%s, error=%s: %s", name, type(e).__name__, e)

    result = {
        "themes": theme_count,
        "theme_stocks": stock_count,
        "unmatched": unmatched_count,
        "failed": failed_count,
        "skipped": False,
    }

    logger.info(
        "테마 RDB 적재 완료: 테마 %d개, 편입 %d개, 미매칭 %d개, 실패 %d개",
        theme_count,
        stock_count,
        unmatched_count,
        failed_count,
    )

    return result
