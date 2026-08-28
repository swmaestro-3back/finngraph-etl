"""투자자별 수급동향 백필.

네이버 trend API를 startIdx 페이징으로 과거까지 거슬러 읽는다. 1페이지가 365건(1년치)이고,
상장 이력이 페이지보다 짧으면 온 만큼만 적재하고 그 종목은 멈춘다.

중간에 죽어도(차단·프로세스 다운) 안전하게 재개되도록 두 가지를 지킨다.

- 종목 하나가 끝날 때마다 커밋한다 — 유실 창이 최대 종목 1개다.
- 시작할 때 종목별 적재 건수를 떠서, 이미 목표 분량(pages × 365)이 있는 종목은 API 호출
  없이 건너뛴다. 재실행하면 저장 안 된 종목부터 실제 호출이 나간다. 상장 이력이 짧아
  목표치에 못 미치는 종목은 매번 다시 조회되지만, 첫 페이지에서 일찍 끝나 낭비가 작다.

수동 실행이 기본이다(DAG schedule=None). 매일 도는 갱신은 collect_investor_flows 가 맡는다.
"""

from __future__ import annotations

from pipelines.common.clients.postgres import session_scope
from pipelines.common.logging import get_logger
from pipelines.companies.loaders.diagnostics import describe_universe
from pipelines.stocks.extractors.naver import BlockSuspectedError, fetch_investor_trends
from pipelines.stocks.loaders.flows import fetch_investor_flow_counts, upsert_investor_flows
from pipelines.stocks.loaders.tickers import fetch_serviceable_stocks

logger = get_logger(__name__)

# 1회 조회 건수. 365건 = 대략 1년치 거래일.
ROWS_PER_PAGE = 365

# 진행 로그 주기. 쿨다운 포함 수십 분짜리 잡이라 중간 경과가 보여야 한다.
PROGRESS_INTERVAL = 100


def run(pages: int = 1, limit: int | None = None) -> None:
    """투자자 수급을 pages년치 백필한다.

    Args:
        pages (int): 종목당 페이지 수. 1페이지가 365건이라 대략 연 단위다.
        limit (int | None): 처리 종목 수 상한. 수동 점검용.
    """

    with session_scope() as session:
        targets = fetch_serviceable_stocks(session, limit)
        if not targets:
            logger.warning("수급 백필 대상이 0종목이다 — %s", describe_universe(session))
        existing_counts = fetch_investor_flow_counts(session)

    target_rows = pages * ROWS_PER_PAGE
    remaining = [
        (stock_id, ticker)
        for stock_id, ticker in targets
        if existing_counts.get(stock_id, 0) < target_rows
    ]
    logger.info(
        "수급 백필 시작: 대상 %d종목 중 %d종목 (%d종목은 이미 %d행 이상이라 건너뜀)",
        len(targets),
        len(remaining),
        len(targets) - len(remaining),
        target_rows,
    )

    total_rows = 0
    failed: list[str] = []

    for index, (_, ticker) in enumerate(remaining, start=1):
        flows = []
        for page in range(pages):
            try:
                # startIdx는 페이지 번호다. page=1이면 366번째 행부터 온다.
                page_flows = fetch_investor_trends(ticker, start_idx=page, page_size=ROWS_PER_PAGE)
            except BlockSuspectedError:
                # 차단 의심은 다음 종목으로 넘어가지 않는다. 계속 두드리면 차단이
                # 길어진다. 종목 단위 커밋이라 재실행하면 여기서부터 이어진다.
                raise
            except Exception:
                logger.exception("수급 백필 실패: ticker=%s page=%d", ticker, page)
                failed.append(ticker)
                break

            flows.extend(page_flows)
            # 페이지가 덜 차면 상장 이력의 끝이다. 더 과거를 물어봐도 빈 응답만 온다.
            if len(page_flows) < ROWS_PER_PAGE:
                break

        if flows:
            with session_scope() as session:
                total_rows += upsert_investor_flows(session, flows)

        if index % PROGRESS_INTERVAL == 0:
            logger.info("수급 백필 진행: %d/%d종목, %d행", index, len(remaining), total_rows)

    logger.info("수급 백필 완료: %d행, 실패 %d종목 %s", total_rows, len(failed), failed[:10])
