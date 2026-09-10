"""테마 Postgres 적재."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.stocks.loaders.tickers import fetch_active_stock_ids

logger = get_logger(__name__)


def _insert_theme(session, name: str, description: str, sources: list[str]) -> int:
    # 기존 테마(merge_themes 가 DB 쪽 이름으로 맞춰 보낸 것)는 description 을 그대로 두고
    # sources 만 합집합으로 늘린다. 순서는 기존 것 뒤에 새 소스가 붙는다.
    row = session.execute(
        text(
            """
            INSERT INTO themes (name, description, sources)
            VALUES (:name, :description, :sources)
            ON CONFLICT (name) DO UPDATE
            SET sources = themes.sources || ARRAY(
                    SELECT s FROM unnest(EXCLUDED.sources) AS s
                     WHERE NOT (s = ANY(themes.sources))
                )
            RETURNING id;
            """
        ),
        {"name": name, "description": description, "sources": sources},
    ).fetchone()

    return row[0]


def _insert_theme_stocks(
    session, theme_id: int, companies: list[dict[str, Any]], stock_ids: dict[str, int]
) -> tuple[int, int]:
    """편입 종목을 추가한다. 이미 편입된 종목은 사유를 덮어쓰지 않는다. (연결 수, 미매칭 수) 반환.

    사유를 보존하는 이유: BELONGS_TO.reason_embedding 은 신규 간선만 임베딩하므로
    (loaders/neo4j.py 의 fetch_reason_embedding_targets) RDB 쪽 사유가 매 회차 바뀌면
    Neo4j 임베딩과 어긋난다. 연결 수에는 이미 있던 종목도 포함된다.
    """

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
                ON CONFLICT (theme_id, stock_id) DO NOTHING;
                """
            ),
            {"theme_id": theme_id, "stock_id": stock_id, "reason": company.get("reason")},
        )

    return linked, unmatched


def load_themes(themes: list[dict[str, Any]]) -> dict[str, Any]:
    """신규 테마와 신규 편입 종목을 추가한다. 기존 themes/theme_stocks 행은 삭제·변경하지 않는다."""

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
