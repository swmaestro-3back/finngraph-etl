"""밸류에이션·수익률 파생 계산.

일봉·재무·배당이 모두 적재된 뒤에 돈다. 계산 자체는 SQL 두 문장이라 종목 수와 무관하게
빠르고, 재실행하면 같은 결과로 덮어쓴다(멱등).

기본은 최근 구간만 다시 계산한다. 전 구간 재계산이 필요하면(재무 백필 직후 등)
lookback_days를 크게 준다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.time import now_kst
from pipelines.stocks.loaders.derived import compute_returns, compute_valuations

logger = get_logger(__name__)

# 최근 며칠을 다시 계산할지. 재무·배당이 늦게 들어와도 반영되도록 넉넉히 잡는다.
DEFAULT_LOOKBACK_DAYS = 30


def run(lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> None:
    """최근 구간의 밸류에이션과 수익률을 계산한다."""

    start_date = now_kst().date() - timedelta(days=lookback_days)

    with session_scope() as session:
        valuations = compute_valuations(session, start_date)
        returns = compute_returns(session, start_date)

    logger.info(
        "파생 계산 완료 (%s 이후): 밸류에이션 %d행, 수익률 %d행",
        start_date,
        valuations,
        returns,
    )
