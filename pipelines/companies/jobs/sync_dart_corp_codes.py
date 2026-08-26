"""DART 고유번호 동기화.

corpCode.xml 한 번 내려받아 두 가지를 한다.

1. 상장 법인에 corp_code를 붙인다 — DART stock_code(단축코드) 기준.
2. 비상장 법인을 corp_code 단위로 전량 적재한다.

주 1회면 충분하다. 8월 4일 118,583건 → 8월 9일 118,681건으로 닷새에 98건 늘었다.
"""

from __future__ import annotations

from pipelines.common.clients.dart import get_dart_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.companies.extractors.dart import fetch_corp_codes
from pipelines.companies.loaders.dart import link_listed_corp_codes, upsert_unlisted_companies

logger = get_logger(__name__)

# 11만 건을 한 트랜잭션에 넣으면 실패 시 전부 되돌아간다. 청크마다 커밋한다.
CHUNK_SIZE = 5000


def run(load_unlisted: bool = True) -> None:
    """corpCode.xml을 받아 상장사 매핑과 비상장 적재를 수행한다.

    Args:
        load_unlisted (bool): 비상장 법인 전량 적재 여부. 끄면 상장사 매핑만 한다.
    """

    client = get_dart_client()
    corps = fetch_corp_codes(client)

    listed = [corp for corp in corps if corp.stock_code]
    logger.info(
        "corpCode 수집: 전체 %d건 (상장 %d, 비상장 %d)",
        len(corps),
        len(listed),
        len(corps) - len(listed),
    )

    linked = 0
    for chunk in chunked(listed, CHUNK_SIZE):
        with session_scope() as session:
            linked += link_listed_corp_codes(session, chunk)

    logger.info("상장사 corp_code 연결: %d건", linked)

    if not load_unlisted:
        return

    unlisted_count = 0
    for chunk in chunked([corp for corp in corps if not corp.stock_code], CHUNK_SIZE):
        with session_scope() as session:
            unlisted_count += upsert_unlisted_companies(session, chunk)

    logger.info("비상장 법인 적재: %d건", unlisted_count)
