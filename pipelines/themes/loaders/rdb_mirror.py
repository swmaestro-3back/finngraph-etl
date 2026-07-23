from __future__ import annotations

from typing import Any

from pipelines.common.logging import get_logger
from pipelines.news.loaders.news_repository import get_connection

logger = get_logger(__name__)

MIN_THEMES_SAFETY = 1


def _upsert_theme(cursor, name: str, description: str, source: str | None) -> int:

    cursor.execute(
        """
        INSERT INTO themes (name, description, source, updated_at, last_seen_at)
        VALUES (%s, %s, %s, now(), now())
        ON CONFLICT (name) DO UPDATE
        SET description = EXCLUDED.description,
            source = EXCLUDED.source,
            updated_at = now(),
            last_seen_at = now()
        RETURNING id;
        """,
        (name, description, source),
    )

    return cursor.fetchone()[0]


def _reconcile_theme_stocks(cursor, theme_id: int, stocks: list[dict[str, Any]]) -> int:

    seen_codes: list[str] = []

    for stock in stocks:
        ticker = (stock.get("ticker") or "").strip()

        if not ticker:
            continue

        seen_codes.append(ticker)

        cursor.execute(
            """
            INSERT INTO theme_stocks (theme_id, stock_code, reason, added_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (theme_id, stock_code) DO UPDATE
            SET reason = EXCLUDED.reason;
            """,
            (theme_id, ticker, stock.get("reason")),
        )

    if seen_codes:
        cursor.execute(
            "DELETE FROM theme_stocks WHERE theme_id = %s AND NOT (stock_code = ANY(%s));",
            (theme_id, seen_codes),
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
    """Neo4j에서 읽은 테마 스냅샷을 RDB themes/theme_stocks에 반영한다.

    themes 항목 형식: {"name", "description", "source", "stocks": [{"ticker", "reason"}]}.
    테마마다 SAVEPOINT로 격리해 개별 실패가 전체를 깨지 않게 한다.
    """

    if len(themes) < MIN_THEMES_SAFETY:
        logger.error("테마 스냅샷이 비어 있어 미러링을 중단합니다(정상 데이터 덮어쓰기 방지).")
        return {"themes": 0, "theme_stocks": 0, "failed": 0, "skipped": True}

    conn = get_connection()
    theme_count = 0
    stock_count = 0
    failed_count = 0

    try:
        with conn:
            with conn.cursor() as cursor:
                for theme in themes:
                    name = (theme.get("name") or "").strip()

                    if not name:
                        continue

                    try:
                        cursor.execute("SAVEPOINT mirror_theme;")

                        theme_id = _upsert_theme(
                            cursor,
                            name=name,
                            description=theme.get("description", "") or "",
                            source=theme.get("source"),
                        )
                        linked = _reconcile_theme_stocks(
                            cursor, theme_id, theme.get("stocks", []) or []
                        )

                        cursor.execute("RELEASE SAVEPOINT mirror_theme;")

                        theme_count += 1
                        stock_count += linked

                    except Exception as e:
                        try:
                            cursor.execute("ROLLBACK TO SAVEPOINT mirror_theme;")
                            cursor.execute("RELEASE SAVEPOINT mirror_theme;")
                        except Exception:
                            conn.rollback()

                        failed_count += 1
                        logger.error(
                            "테마 미러 실패: name=%s, error=%s: %s", name, type(e).__name__, e
                        )

    finally:
        conn.close()

    result = {
        "themes": theme_count,
        "theme_stocks": stock_count,
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
