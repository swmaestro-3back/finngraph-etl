"""개체 사전(entity_gazetteer) 적재."""

from __future__ import annotations

from dataclasses import asdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.companies.models import GazetteerAlias

COUNT_GAZETTEER_SQL = text("SELECT count(*) FROM entity_gazetteer")

# 전량 교체는 한 트랜잭션 안의 DELETE + INSERT 다. TRUNCATE 는 읽는 쪽까지 잠그지만, DELETE 는
# 커밋 전까지 읽는 쪽이 이전 스냅샷을 그대로 본다.
DELETE_GAZETTEER_SQL = text("DELETE FROM entity_gazetteer")

INSERT_GAZETTEER_SQL = text(
    """
    INSERT INTO entity_gazetteer (alias, company_id, stock_id, ticker, canonical_name, alias_source)
    VALUES (:alias, :company_id, :stock_id, :ticker, :canonical_name, :source)
    """
)

# 새 사전이 기존의 이 비율보다 작으면 교체하지 않는다. 마스터가 반쯤 비어 들어온 날 사전이
# 같이 쪼그라들면, 그날 수집된 뉴스의 기업 연결이 조용히 빠진다.
MIN_RETAIN_RATIO = 0.5


def replace_gazetteer(session: Session, entries: list[GazetteerAlias]) -> int:
    """entity_gazetteer 를 entries 로 통째로 바꾸고 이전 행 수를 돌려준다.

    새 사전이 이전의 MIN_RETAIN_RATIO 미만이면 바꾸지 않고 예외를 던진다. 커밋은
    호출자(session_scope)가 한다.
    """

    previous = int(session.execute(COUNT_GAZETTEER_SQL).scalar_one())
    if len(entries) < previous * MIN_RETAIN_RATIO:
        raise RuntimeError(
            f"개체 사전이 {previous}건 → {len(entries)}건으로 줄어 교체하지 않는다 — "
            "마스터 동기화 결과를 먼저 확인"
        )

    session.execute(DELETE_GAZETTEER_SQL)
    if entries:
        session.execute(INSERT_GAZETTEER_SQL, [asdict(entry) for entry in entries])
    return previous
