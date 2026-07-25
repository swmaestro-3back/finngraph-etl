from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.database import session_scope
from pipelines.common.logging import get_logger

logger = get_logger(__name__)

MIN_THEMES_SAFETY = 1


def _upsert_theme(session, name: str, description: str) -> int:

    row = session.execute(
        text(
            """
            INSERT INTO themes (name, description, updated_at, last_seen_at)
            VALUES (:name, :description, now(), now())
            ON CONFLICT (name) DO UPDATE
            SET description = EXCLUDED.description,
                updated_at = now(),
                last_seen_at = now()
            RETURNING id;
            """
        ),
        {"name": name, "description": description},
    ).fetchone()

    return row[0]


def _reconcile_theme_companies(session, theme_id: int, stocks: list[dict[str, Any]]) -> int:

    seen_codes: list[str] = []

    for stock in stocks:
        ticker = (stock.get("ticker") or "").strip()

        if not ticker:
            continue

        seen_codes.append(ticker)

        session.execute(
            text(
                """
                INSERT INTO theme_companies (theme_id, stock_code, reason, added_at)
                VALUES (:theme_id, :stock_code, :reason, now())
                ON CONFLICT (theme_id, stock_code) DO UPDATE
                SET reason = EXCLUDED.reason;
                """
            ),
            {"theme_id": theme_id, "stock_code": ticker, "reason": stock.get("reason")},
        )

    if seen_codes:
        session.execute(
            text(
                "DELETE FROM theme_companies "
                "WHERE theme_id = :theme_id AND NOT (stock_code = ANY(:codes));"
            ),
            {"theme_id": theme_id, "codes": seen_codes},
        )
    else:
        # 빈 스냅샷은 "테마에 종목이 없어짐"과 "이번 크롤에서 종목을 못 읽음"(부분 실행/라벨 누락)을
        # 구분할 수 없다. 후자에서 정상 편입을 통째로 지우는 것을 막기 위해, 빈 경우엔 삭제하지 않고
        # 기존 편입을 보존한다(정상 데이터 유실 위험 > 사라진 테마의 stale 편입이 남는 위험).
        logger.warning(
            "테마 편입 스냅샷이 비어 삭제를 건너뜀(기존 편입 보존): theme_id=%s", theme_id
        )

    return len(seen_codes)


def mirror_themes_to_rdb(themes: list[dict[str, Any]]) -> dict[str, Any]:
    """Neo4j에서 읽은 테마 스냅샷을 RDB themes/theme_companies에 반영한다.

    themes 항목 형식: {"name", "description", "stocks": [{"ticker", "reason"}]}.
    테마마다 SAVEPOINT(begin_nested)로 격리해 개별 실패가 전체를 깨지 않게 한다.
    """

    if len(themes) < MIN_THEMES_SAFETY:
        logger.error("테마 스냅샷이 비어 있어 미러링을 중단합니다(정상 데이터 덮어쓰기 방지).")
        return {"themes": 0, "theme_companies": 0, "failed": 0, "skipped": True}

    theme_count = 0
    stock_count = 0
    failed_count = 0

    with session_scope() as session:
        for theme in themes:
            name = (theme.get("name") or "").strip()

            if not name:
                continue

            try:
                with session.begin_nested():
                    theme_id = _upsert_theme(
                        session,
                        name=name,
                        description=theme.get("description", "") or "",
                    )
                    linked = _reconcile_theme_companies(
                        session, theme_id, theme.get("stocks", []) or []
                    )

                theme_count += 1
                stock_count += linked

            except Exception as e:
                failed_count += 1
                logger.error("테마 미러 실패: name=%s, error=%s: %s", name, type(e).__name__, e)

    result = {
        "themes": theme_count,
        "theme_companies": stock_count,
        "failed": failed_count,
        "skipped": False,
    }

    logger.info(
        "theme_mirror 적재 완료: 테마 %d개, 편입 %d개, 실패 %d개",
        theme_count,
        stock_count,
        failed_count,
    )

    return result
