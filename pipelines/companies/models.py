from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompanySyncResult:
    """국내 상장 기업 마스터 동기화 결과.

    Attributes:
        upserted_count (int): companies에 삽입되거나 갱신된 법인 수.
        linked_count (int): stocks.company_id가 새로 연결되거나 바뀐 종목 수.
            이미 같은 법인을 가리키던 종목은 세지 않는다.
        alias_count (int): company_aliases에 새로 추가된 별칭 수.
            이미 있던 별칭은 ON CONFLICT DO NOTHING으로 빠지므로 세지 않는다.
    """

    upserted_count: int
    linked_count: int
    alias_count: int
