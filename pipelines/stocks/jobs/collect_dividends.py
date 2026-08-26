"""배당 수집.

KIS `ksdinfo/dividend`(예탁원 정보)에서 주당배당금(DPS)·기준일·지급일을 받는다.
프론트의 dividendYield는 여기서 나온 DPS를 주가로 나눠 파생 계산 단계에서 만든다.

배당은 분기에 한 번 바뀌므로 주 1회면 충분하다. 기준일 구간을 몇 년으로 넉넉히 잡아
과거 배당 이력도 함께 채운다 — 배당수익률을 과거 시점으로 계산하려면 이력이 필요하다.
"""

from __future__ import annotations

from datetime import timedelta

from pipelines.common.clients.kis import get_kis_client
from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.common.utils.batching import chunked
from pipelines.common.utils.time import now_kst
from pipelines.companies.loaders.diagnostics import describe_universe
from pipelines.stocks.extractors.kis import fetch_dividends
from pipelines.stocks.loaders.flows import upsert_dividends
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks

logger = get_logger(__name__)

CHUNK_SIZE = 100

# 조회 구간 기본값. 배당수익률 시계열을 만들려면 몇 해치가 필요하다.
DEFAULT_LOOKBACK_DAYS = 365 * 3


def run(limit: int | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> None:
    """배당 이력을 수집한다.

    대상을 자르지 않는다. 목록이 단축코드 순으로 고정이라 상한을 두면 뒤쪽 종목에 순서가
    영영 오지 않는다. 종목당 1회라 전량을 돌아도 360여 회다.

    Args:
        limit (int | None): 상한. 수동 점검용이고 배치에서는 쓰지 않는다.
        lookback_days (int): 조회 구간 길이.
    """

    client = get_kis_client()
    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)
        if not targets:
            logger.warning("배당 수집 대상이 0종목이다 — %s", describe_universe(session))

    today = now_kst().date()
    start = today - timedelta(days=lookback_days)

    logger.info("배당 수집 시작: 대상 %d종목, 구간 %s~%s", len(targets), start, today)

    total_rows = 0
    failed: list[str] = []

    for chunk in chunked(targets, CHUNK_SIZE):
        dividends = []
        for _, ticker in chunk:
            try:
                dividends.extend(fetch_dividends(ticker, start, today, client=client))
            except Exception:
                logger.exception("배당 수집 실패: ticker=%s", ticker)
                failed.append(ticker)

        if dividends:
            with session_scope() as session:
                total_rows += upsert_dividends(session, dividends)

    logger.info("배당 수집 완료: %d행, 실패 %d종목 %s", total_rows, len(failed), failed[:10])
