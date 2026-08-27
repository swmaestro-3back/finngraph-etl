"""네이버 증권 투자자 수급 수집.

`stock.naver.com`의 trend API가 일별 개인·기관·외국인 순매수량과 외국인 보유율을
한 응답에 준다. startIdx/pageSize 페이징이라 최신 갱신과 과거 백필을 같은 엔드포인트로
처리한다.

비공식 API라 응답 형태가 예고 없이 바뀔 수 있다. 필수 키가 빠지면 건너뛰지 않고 즉시
실패시킨다 — 조용히 넘어가면 빈 데이터가 정상 수집처럼 쌓인다. 403·429나 JSON 이 아닌
응답은 차단 신호로 보고 BlockSuspectedError 를 던진다. job 은 이 예외를 삼키지 말고
즉시 중단해야 한다 — 차단된 뒤 계속 두드리면 임시 차단이 길어진다.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import requests

from pipelines.common.utils.rate_limit import BurstPacer
from pipelines.stocks.models import InvestorFlow

TREND_URL = "https://stock.naver.com/api/domestic/detail/{ticker}/trend"

# 브라우저 UA가 아니면 차단될 수 있다.
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

REQUEST_TIMEOUT_SECONDS = 10

# 차단 신호로 취급하는 상태 코드. 429는 명시적 과다 호출, 403은 봇 판정이다.
BLOCK_STATUS_CODES = (403, 429)

# 공식 한도가 공개돼 있지 않다. 요청 간격에 지터를 주고(평균 ~1.3회/초) 200회마다
# 길게 쉬어, 일정한 기계적 패턴과 윈도우당 과다 호출 둘 다 피한다.
_pacer = BurstPacer(
    min_interval_seconds=0.4,
    max_interval_seconds=1.2,
    burst_size=200,
    min_cooldown_seconds=45,
    max_cooldown_seconds=90,
)

# keep-alive 재사용. 호출마다 새 TCP 연결을 맺는 것보다 브라우저 동작에 가깝다.
_session = requests.Session()


class BlockSuspectedError(RuntimeError):
    """네이버가 요청을 거절했다(IP 차단 의심). 재시도하지 말고 잡을 중단해야 한다."""


# 보유율 원천 값에 float 잡음이 섞여 온다(46.709999084472656). 소수 4자리로 정규화한다.
_RATIO_EXPONENT = Decimal("0.0001")


def fetch_investor_trends(
    ticker: str,
    start_idx: int = 0,
    page_size: int = 365,
) -> list[InvestorFlow]:
    """일별 투자자 수급을 가져온다.

    Args:
        ticker (str): 단축코드.
        start_idx (int): 페이지 번호(행 오프셋이 아니다 — 2026-08-28 실측). 0이 최신
            페이지이고, 실제 건너뛰는 행 수는 start_idx × page_size 다.
        page_size (int): 1회 조회 건수.

    Returns:
        list[InvestorFlow]: 거래일 오름차순. 상장 이력이 짧으면 page_size보다 적게 온다.
    """

    _pacer.wait()
    response = _session.get(
        TREND_URL.format(ticker=ticker),
        params={"tradeType": "KRX", "startIdx": start_idx, "pageSize": page_size},
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code in BLOCK_STATUS_CODES:
        raise BlockSuspectedError(
            f"네이버가 요청을 거절했다: ticker={ticker} status={response.status_code}"
        )
    response.raise_for_status()

    try:
        rows = response.json()
    except ValueError as error:
        # 차단·캡차 페이지는 HTML로 온다.
        raise BlockSuspectedError(
            f"응답이 JSON이 아니다(차단·캡차 의심): ticker={ticker}"
        ) from error

    return parse_trend_rows(ticker, rows)


def parse_trend_rows(ticker: str, rows: object) -> list[InvestorFlow]:
    """trend API 응답을 InvestorFlow로 바꾼다.

    응답이 리스트가 아니거나 행에 필수 키가 없으면 예외를 던진다(응답 형태 변경 감지).
    """

    if not isinstance(rows, list):
        raise ValueError(
            f"trend 응답이 리스트가 아니다: ticker={ticker} type={type(rows).__name__}"
        )

    flows: list[InvestorFlow] = []
    for row in rows:
        trade_date = _parse_date(row["bizdate"])
        if trade_date is None:
            raise ValueError(f"bizdate를 해석할 수 없다: ticker={ticker} value={row['bizdate']!r}")

        flows.append(
            InvestorFlow(
                ticker=ticker,
                trade_date=trade_date,
                individual_net_qty=_parse_int(row["individualPureBuyQuant"]),
                institution_net_qty=_parse_int(row["organPureBuyQuant"]),
                foreign_net_qty=_parse_int(row["foreignerPureBuyQuant"]),
                foreign_hold_ratio=_parse_ratio(row["frgnHoldRatio"]),
            )
        )

    return sorted(flows, key=lambda flow: flow.trade_date)


# -- 내부 --------------------------------------------------------------------


# 값 파싱도 키 누락과 같은 원칙이다 — 결측(빈 문자열)만 None으로 허용하고, 값이 있는데
# 해석이 안 되면 포맷 변경으로 보고 즉시 실패한다. None으로 삼키면 전 종목 수량이
# NULL인 채 정상 수집처럼 적재된다.


def _parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_int(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(Decimal(text))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"수량을 해석할 수 없다: {value!r}") from error


def _parse_ratio(value: object) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text).quantize(_RATIO_EXPONENT)
    except InvalidOperation as error:
        raise ValueError(f"보유율을 해석할 수 없다: {value!r}") from error
