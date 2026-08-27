"""테마 임베딩 대상 조회·적재.

테마 리프레시가 전량 삭제-재적재라 매 리프레시 후 모든 행이 embedding NULL 로
시작한다 — 사실상 매 회차 전량 재임베딩이다. 스테일 판정(embedding NULL 또는
해시 불일치)은 임베딩 잡이 중간에 죽었을 때 이미 적재한 청크를 건너뛰고 재개하기
위한 장치다. 해시는 SELECT 시점에 SQL 로 계산해 그대로 UPDATE 에 되쓴다 —
판정과 기록이 같은 표현식에서 나와야 Python/SQL 간 해시 표현이 어긋날 수 없다.

임베딩 차원은 vector(1024) 스키마와 결합된 값이라 설정이 아닌 상수다. 바꾸려면
마이그레이션(타입 변경 + embedding 전량 NULL 리셋)이 함께 필요하다.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

EMBEDDING_DIM = 1024


def _to_vector_literal(embedding: list[float]) -> str:
    if len(embedding) != EMBEDDING_DIM:
        raise ValueError(f"임베딩 차원 불일치: {len(embedding)} != {EMBEDDING_DIM}")
    return "[" + ",".join(str(value) for value in embedding) + "]"


def fetch_stale_themes(session: Session) -> list[dict[str, Any]]:
    """임베딩이 없거나 name·description 이 바뀐 테마를 (id, text, text_hash)로 반환한다."""

    rows = session.execute(
        text(
            """
            SELECT id, t.text, md5(t.text) AS text_hash
            FROM themes,
                 LATERAL (SELECT name || E'\n' || coalesce(description, '') AS text) t
            WHERE embedding IS NULL
               OR embedding_text_hash IS DISTINCT FROM md5(t.text)
            ORDER BY id;
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def fetch_stale_reasons(session: Session) -> list[dict[str, Any]]:
    """임베딩이 없거나 reason 이 바뀐 테마 편입 행을 (id, text, text_hash)로 반환한다."""

    rows = session.execute(
        text(
            """
            SELECT id, reason AS text, md5(reason) AS text_hash
            FROM theme_stocks
            WHERE reason IS NOT NULL
              AND (reason_embedding IS NULL
                   OR reason_text_hash IS DISTINCT FROM md5(reason))
            ORDER BY id;
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def update_theme_embeddings(session: Session, rows: list[dict[str, Any]]) -> int:
    """(id, embedding, text_hash) 목록을 themes 에 기록한다. text_hash 는 fetch 가 준 값이다."""

    if not rows:
        return 0
    session.execute(
        text(
            """
            UPDATE themes
            SET embedding = (:embedding)::vector, embedding_text_hash = :text_hash
            WHERE id = :id;
            """
        ),
        [
            {
                "id": row["id"],
                "embedding": _to_vector_literal(row["embedding"]),
                "text_hash": row["text_hash"],
            }
            for row in rows
        ],
    )
    return len(rows)


def fetch_theme_embeddings(session: Session) -> list[dict[str, Any]]:
    """임베딩이 있는 전 테마를 (name, embedding)으로 반환한다. Neo4j 동기화 재료다.

    Neo4j Theme 노드의 병합 키가 name 이라 id 가 아닌 name 으로 준다.
    """

    rows = session.execute(
        text(
            """
            SELECT name, embedding::text AS embedding
            FROM themes
            WHERE embedding IS NOT NULL
            ORDER BY id;
            """
        )
    ).mappings()
    return [{"name": row["name"], "embedding": json.loads(row["embedding"])} for row in rows]


def update_reason_embeddings(session: Session, rows: list[dict[str, Any]]) -> int:
    """(id, embedding, text_hash) 목록을 theme_stocks 에 기록한다."""

    if not rows:
        return 0
    session.execute(
        text(
            """
            UPDATE theme_stocks
            SET reason_embedding = (:embedding)::vector, reason_text_hash = :text_hash
            WHERE id = :id;
            """
        ),
        [
            {
                "id": row["id"],
                "embedding": _to_vector_literal(row["embedding"]),
                "text_hash": row["text_hash"],
            }
            for row in rows
        ],
    )
    return len(rows)
