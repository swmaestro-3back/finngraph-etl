"""단일판매ㆍ공급계약체결 공시 백필. 오늘부터 소급 연수만큼 거슬러 수집하고, 이어서
그래프 간선까지 반영한다.

일 API 한도나 회차 상한에 걸려 끊겨도 재실행하면 이어진다 — 이미 적재된 접수번호는
원문을 다시 받지 않는다. 그래프 연결은 전량 재구성이라 끊긴 회차에 돌아도 그 시점까지
적재된 만큼은 반영된다.
"""

from __future__ import annotations

from pipelines.common.config import get_settings
from pipelines.common.utils.time import now_kst
from pipelines.disclosures.jobs import collect_supply_contracts, link_supply_contracts


def run(batch_size: int | None = None) -> None:
    """오늘로부터 disclosure_backfill_years년 전 ~ 오늘 구간을 수집하고 그래프에 잇는다."""

    settings = get_settings()
    end = now_kst().date()
    try:
        start = end.replace(year=end.year - settings.disclosure_backfill_years)
    except ValueError:  # 2월 29일
        start = end.replace(year=end.year - settings.disclosure_backfill_years, day=28)

    collect_supply_contracts.run(start, end, batch_size=batch_size)
    link_supply_contracts.run()
