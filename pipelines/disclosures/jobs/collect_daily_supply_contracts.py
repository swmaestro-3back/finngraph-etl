"""단일판매ㆍ공급계약체결 공시 일일 수집 후 그래프 간선 반영.

전일 공시가 목표지만 구간을 lookback 만큼 되짚는다 — 장애로 빠진 날을 다음 실행이
자연히 메우고, 겹침 비용은 목록 API 몇 번뿐이다(적재된 접수번호는 원문을 안 받는다).
정정 공시는 새 접수번호로 오므로 같은 경로로 잡힌다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.config import get_settings
from pipelines.common.utils.time import now_kst
from pipelines.disclosures.jobs import collect_supply_contracts, link_supply_contracts


def run() -> None:
    """오늘로부터 disclosure_daily_lookback_days일 전 ~ 오늘 구간을 수집하고 그래프에 잇는다."""

    settings = get_settings()
    end = now_kst().date()
    start = end - timedelta(days=settings.disclosure_daily_lookback_days)

    collect_supply_contracts.run(start, end)
    link_supply_contracts.run()
