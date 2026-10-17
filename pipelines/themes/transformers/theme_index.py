"""테마 지수 계산 — 순수 함수.

DB 를 모른다. 구성 종목 일봉 행을 받아 시총 가중(종목당 상한) 체인 링크 지수를 돌려준다.
로더가 조회·적재를 맡고, 여기서는 규칙만 다룬다.

시총 가중에 상한을 두는 이유는 대형주 한 종목이 테마 지수를 그 종목 차트로 만들어 버리기
때문이다. 상한을 넘는 비중은 나머지 종목에 원시 시총 비율로 다시 나눈다.
"""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from pipelines.themes.types import ConstituentCandle, ThemeCandle

BASE_INDEX = Decimal("1000")
DEFAULT_CAP = Decimal("0.25")
# KR 가격제한폭(±30%). 이를 넘는 하루 수익률은 실제 등락이 아니라 수정주가 리프레시 경계(t-1 은
# 미조정·t 는 조정)일 가능성이 높아 그날 제외한다. 정리매매·거래재개일의 큰 등락도 하루 빠지는
# 것은 감수한다.
MAX_DAILY_RETURN = Decimal("0.30")
_QUANT = Decimal("0.0001")


def cap_weights(mcaps: dict[int, Decimal], cap: Decimal = DEFAULT_CAP) -> dict[int, Decimal]:
    """시총 비례 가중치에 종목당 상한을 적용한다.

    Args:
        mcaps (dict[int, Decimal]): stock_id → 시가총액. 모두 0 보다 커야 한다.
        cap (Decimal): 종목당 비중 상한.

    Returns:
        dict[int, Decimal]: stock_id → 가중치. 합은 1. n × cap > 1 이면 모든 값이 cap 이하이고,
            n × cap ≤ 1(종목 4개 이하)이면 동일 가중 1/n 이라 cap 을 넘을 수 있다.
    """

    if not mcaps:
        return {}

    n = len(mcaps)
    if n * cap <= 1:
        # n ≤ 4 에서 상한을 지키는 유일한 해는 동일 가중이다.
        equal = Decimal(1) / Decimal(n)
        return {stock_id: equal for stock_id in mcaps}

    fixed: set[int] = set()
    while True:
        remaining = Decimal(1) - cap * len(fixed)
        free_total = sum(m for stock_id, m in mcaps.items() if stock_id not in fixed)
        weights = {
            stock_id: cap if stock_id in fixed else m / free_total * remaining
            for stock_id, m in mcaps.items()
        }
        over = [s for s, w in weights.items() if s not in fixed and w > cap]
        if not over:
            return weights
        fixed.update(over)


def _q(value: Decimal) -> Decimal:
    return value.quantize(_QUANT, rounding=ROUND_HALF_UP)


def compute_theme_candles(
    theme_id: int,
    rows: list[ConstituentCandle],
    calendar: list[date],
    start: date,
    end: date,
    anchor_close: Decimal | None,
    cap: Decimal = DEFAULT_CAP,
) -> list[ThemeCandle]:
    """구간 [start, end] 의 테마 지수 일봉을 체인 링크로 계산한다.

    거래일 t 의 지수는 직전 지수 종가 P 에 구성 종목의 t-1 → t 수익률을 가중 합해 만든다.
    t-1 은 시장 캘린더(calendar) 기준이다. t-1 이나 t 봉이 없는 종목, listed_shares 가
    없는 종목은 그날만 빠지고 나머지로 비중을 다시 나눈다. 하루 수익률 절댓값이
    MAX_DAILY_RETURN(가격제한폭)을 넘는 종목도 그날만 빠진다. 참여 종목이 0 이면 행을 만들지
    않고 P 를 그대로 다음 날로 넘긴다.

    Args:
        rows (list[ConstituentCandle]): start 직전 거래일부터 end 까지의 구성 종목 일봉.
        calendar (list[date]): 오름차순 시장 거래일. start 직전 거래일을 포함해야 첫날이 계산된다.
        anchor_close (Decimal | None): start 이전 마지막 테마 종가. 없으면 첫 계산 가능일을
            BASE_INDEX 에서 시작한다.

    Returns:
        list[ThemeCandle]: 날짜 오름차순. 지수값은 소수 4 자리로 반올림된다.
    """

    by_date: dict[date, dict[int, ConstituentCandle]] = {}
    for row in rows:
        by_date.setdefault(row.trade_date, {})[row.stock_id] = row

    level: Decimal | None = anchor_close
    out: list[ThemeCandle] = []

    for idx, day in enumerate(calendar):
        if day < start or day > end or idx == 0:
            continue
        today = by_date.get(day, {})
        yesterday = by_date.get(calendar[idx - 1], {})

        parts = [
            (today[s], yesterday[s])
            for s in today
            if s in yesterday
            and yesterday[s].close > 0
            and abs(today[s].close / yesterday[s].close - 1) <= MAX_DAILY_RETURN
            and today[s].listed_shares is not None
            and today[s].listed_shares > 0
        ]
        if not parts:
            continue

        if level is None:
            level = BASE_INDEX

        weights = cap_weights({t.stock_id: y.close * t.listed_shares for t, y in parts}, cap)

        def blend(field: str, level=level, weights=weights, parts=parts) -> Decimal:
            return level * sum(weights[t.stock_id] * getattr(t, field) / y.close for t, y in parts)

        op, hi, lo, cl = blend("open"), blend("high"), blend("low"), blend("close")
        hi = max(op, hi, lo, cl)
        lo = min(op, hi, lo, cl)

        trade_values = [t.trade_value for t, _ in parts if t.trade_value is not None]
        out.append(
            ThemeCandle(
                theme_id=theme_id,
                trade_date=day,
                open=_q(op),
                high=_q(hi),
                low=_q(lo),
                close=_q(cl),
                volume=sum(t.volume for t, _ in parts),
                trade_value=sum(trade_values) if trade_values else None,
            )
        )
        level = cl

    return out
