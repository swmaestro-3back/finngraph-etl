"""테마 Postgres 적재."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.loaders.tickers import fetch_active_stock_ids

logger = get_logger(__name__)

# 전량 삭제-재적재 앞의 안전장치. 크롤러가 부분적으로 깨져 소수의 테마만 파싱돼도
# 기존 데이터를 날리지 않도록, 절대 개수와 기존 대비 비율을 함께 본다.
MIN_THEMES_SAFETY = 50
MIN_THEMES_RATIO = 0.5


def _insert_theme(session, name: str, description: str) -> int:
    # 스냅샷 내 중복 테마명은 한 행으로 병합한다.
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

    if len(themes) < MIN_THEMES_SAFETY:
        logger.error(
            "테마 스냅샷이 %d개뿐이라 적재를 중단합니다(정상 데이터 덮어쓰기 방지).",
            len(themes),
        )
        return {"themes": 0, "theme_stocks": 0, "unmatched": 0, "failed": 0, "skipped": True}

    theme_count = 0
    stock_count = 0
    unmatched_count = 0
    failed_count = 0

    with session_scope() as session:
        stock_ids = fetch_active_stock_ids(session)

        # 부분 크롤 감지 — 기존 대비 급감한 스냅샷은 결측으로 보고 기존 데이터를 보존한다.
        existing = session.execute(text("SELECT count(*) FROM themes;")).scalar_one()
        if existing and len(themes) < existing * MIN_THEMES_RATIO:
            logger.error(
                "테마 스냅샷이 기존 %d개 대비 %d개로 급감해 적재를 중단합니다(크롤 결측 의심).",
                existing,
                len(themes),
            )
            return {
                "themes": 0,
                "theme_stocks": 0,
                "unmatched": 0,
                "failed": 0,
                "skipped": True,
            }

        # 전량 삭제-재적재. theme_stocks 는 FK ON DELETE CASCADE 로 함께 지워진다.
        session.execute(text("DELETE FROM themes;"))

        for theme in themes:
            name = (theme.get("name") or "").strip()

            if not name:
                continue

            try:
                with session.begin_nested():
                    theme_id = _insert_theme(
                        session, name=name, description=theme.get("description", "") or ""
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
