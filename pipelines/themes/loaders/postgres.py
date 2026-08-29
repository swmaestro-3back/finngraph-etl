"""테마 Postgres 적재."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.loaders.tickers import fetch_active_stock_ids

logger = get_logger(__name__)


def _insert_theme(session, name: str, description: str, sources: list[str]) -> int:
    # 스냅샷 내 중복 테마명은 한 행으로 병합한다.
    row = session.execute(
        text(
            """
            INSERT INTO themes (name, description, sources)
            VALUES (:name, :description, :sources)
            ON CONFLICT (name) DO UPDATE
            SET description = EXCLUDED.description,
                sources = EXCLUDED.sources
            RETURNING id;
            """
        ),
        {"name": name, "description": description, "sources": sources},
    ).fetchone()

    return row[0]


def _insert_theme_stocks(
    session, theme_id: int, companies: list[dict[str, Any]], stock_ids: dict[str, int]
) -> tuple[int, int]:
    """스냅샷의 편입 종목을 적재한다. (연결 수, 미매칭 수) 반환."""

    linked = 0
    unmatched = 0

    for company in companies:
        ticker = (company.get("ticker") or "").strip()
        stock_id = stock_ids.get(ticker)

        if not stock_id:
            unmatched += 1
            continue

        linked += 1
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

    return linked, unmatched


def load_themes(themes: list[dict[str, Any]]) -> dict[str, Any]:

    theme_count = 0
    stock_count = 0
    unmatched_count = 0
    failed_count = 0

    with session_scope() as session:
        stock_ids = fetch_active_stock_ids(session)

        # 전량 삭제-재적재. theme_stocks 는 FK ON DELETE CASCADE 로 함께 지워진다.
        session.execute(text("DELETE FROM themes;"))

        for theme in themes:
            name = (theme.get("name") or "").strip()

            if not name:
                continue

            try:
                with session.begin_nested():
                    theme_id = _insert_theme(
                        session,
                        name=name,
                        description=theme.get("description", "") or "",
                        sources=theme.get("sources") or [],
                    )
                    linked, unmatched = _insert_theme_stocks(
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
    }

    logger.info(
        "테마 RDB 적재 완료: 테마 %d개, 편입 %d개, 미매칭 %d개, 실패 %d개",
        theme_count,
        stock_count,
        unmatched_count,
        failed_count,
    )

    return result
