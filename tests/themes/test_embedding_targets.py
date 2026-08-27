"""임베딩 대상 선정(스테일 판정) 순수 로직 테스트.

임베딩 원천이 Neo4j 로 옮겨지며 스테일 판정(해시 비교)은 SQL 이 아니라 Python
순수 함수가 됐다 — 판정과 기록이 같은 함수(text_hash)에서 나온다는 계약은 동일하다.
리프레시가 전량 삭제-재적재라 실전에서는 매 회차 전량이 스테일이고, 이 판정은
잡 중간 실패 시 이미 기록한 청크를 건너뛰고 재개하는 기준이다.
"""

from __future__ import annotations

import hashlib

from pipelines.themes.loaders.embeddings import stale_targets, text_hash, theme_text


def _row(text: str, stored_hash: str | None, has_embedding: bool) -> dict:
    return {
        "name": "테마",
        "text": text,
        "stored_hash": stored_hash,
        "has_embedding": has_embedding,
    }


def test_theme_text_joins_name_and_description():
    assert theme_text("2차전지", "배터리 밸류체인") == "2차전지\n배터리 밸류체인"


def test_theme_text_without_description_keeps_name_only():
    assert theme_text("2차전지", None) == "2차전지\n"


def test_text_hash_is_md5_hex():
    assert text_hash("가나다") == hashlib.md5("가나다".encode()).hexdigest()


def test_row_without_embedding_is_stale():
    stale = stale_targets([_row("사유", stored_hash=None, has_embedding=False)])

    assert len(stale) == 1
    assert stale[0]["text_hash"] == text_hash("사유")


def test_embedded_row_with_matching_hash_is_not_stale():
    row = _row("사유", stored_hash=text_hash("사유"), has_embedding=True)

    assert stale_targets([row]) == []


def test_embedded_row_with_changed_text_is_stale_again():
    row = _row("바뀐 사유", stored_hash=text_hash("옛 사유"), has_embedding=True)

    stale = stale_targets([row])

    assert len(stale) == 1
    assert stale[0]["text_hash"] == text_hash("바뀐 사유")


def test_hash_without_embedding_is_still_stale():
    # 해시만 남고 벡터가 지워진 비정상 상태 — 보수적으로 재임베딩한다.
    row = _row("사유", stored_hash=text_hash("사유"), has_embedding=False)

    assert len(stale_targets([row])) == 1
