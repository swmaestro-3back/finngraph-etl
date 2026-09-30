"""테마 지수 순수 함수 테스트. DB 없이 손계산 값과 비교한다."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pipelines.themes.transformers.theme_index import (
    BASE_INDEX,
    cap_weights,
    compute_theme_candles,
)
from pipelines.themes.types import ConstituentCandle


def _close(a: Decimal, b: str) -> bool:
    return abs(a - Decimal(b)) < Decimal("1e-9")


def test_cap_weights_equal_weight_when_four_or_fewer() -> None:
    """n×cap ≤ 1 이면 상한을 지키는 유일한 해는 동일 가중이다."""
    weights = cap_weights({1: Decimal(90), 2: Decimal(5), 3: Decimal(3), 4: Decimal(2)})

    assert all(_close(w, "0.25") for w in weights.values())

    two = cap_weights({1: Decimal(90), 2: Decimal(10)})
    assert all(_close(w, "0.5") for w in two.values())


def test_cap_weights_caps_dominant_stock_and_redistributes_pro_rata() -> None:
    """40% 종목만 25% 로 잘리고, 나머지 75% 는 원시 시총 비율대로 한 번에 배분된다(연쇄 없음)."""
    mcaps = {1: Decimal(40), 2: Decimal(15), 3: Decimal(15), 4: Decimal(15), 5: Decimal(15)}

    weights = cap_weights(mcaps)

    assert _close(weights[1], "0.25")
    # 남은 0.75 를 15:15:15:15 로 → 각 0.1875, 모두 cap 이하라 재배분은 한 번으로 끝난다.
    for stock_id in (2, 3, 4, 5):
        assert _close(weights[stock_id], "0.1875")


def test_cap_weights_cascades_until_no_stock_exceeds_cap() -> None:
    """1차 재배분으로 새로 25% 를 넘는 종목도 결국 상한 이하가 된다."""
    mcaps = {1: Decimal(50), 2: Decimal(20), 3: Decimal(15), 4: Decimal(10), 5: Decimal(5)}

    weights = cap_weights(mcaps)

    # 1차: 1번(0.5) 고정 → 2번이 0.75×20/50 = 0.30 으로 새로 초과 → 2번도 고정.
    # 남은 0.5 를 15:10:5 로 나눈다.
    assert _close(weights[1], "0.25")
    assert _close(weights[2], "0.25")
    assert _close(weights[3], "0.25")
    assert _close(weights[4], "0.1666666667")
    assert _close(weights[5], "0.0833333333")
    assert all(w <= Decimal("0.25") + Decimal("1e-12") for w in weights.values())


def test_cap_weights_sum_to_one() -> None:
    mcaps = {i: Decimal(v) for i, v in enumerate((100, 40, 30, 20, 10, 5, 1), start=1)}

    total = sum(cap_weights(mcaps).values())

    assert _close(total, "1")


def test_cap_weights_empty() -> None:
    assert cap_weights({}) == {}


D1, D2, D3 = date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)
CAL = [D1, D2, D3]


def _row(
    stock_id: int,
    day: date,
    close: str,
    *,
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
    volume: int = 10,
    trade_value: int | None = 100,
    listed_shares: int | None = 1000,
) -> ConstituentCandle:
    c = Decimal(close)
    return ConstituentCandle(
        stock_id=stock_id,
        trade_date=day,
        open=Decimal(open_) if open_ else c,
        high=Decimal(high) if high else c,
        low=Decimal(low) if low else c,
        close=c,
        volume=volume,
        trade_value=trade_value,
        listed_shares=listed_shares,
    )


def test_two_stocks_three_days_matches_hand_calculation() -> None:
    """n ≤ 4 라 동일 가중. D2: (1.10 + 0.90)/2 = 1.00, D3: (1.00 + 1.20)/2 = 1.10."""
    rows = [
        _row(1, D1, "100"),
        _row(2, D1, "200"),
        _row(1, D2, "110"),
        _row(2, D2, "180"),
        _row(1, D3, "110"),
        _row(2, D3, "216"),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D3, anchor_close=None)

    assert [c.trade_date for c in out] == [D2, D3]
    assert out[0].close == Decimal("1000.0000")
    assert out[1].close == Decimal("1100.0000")
    assert out[0].volume == 20
    assert out[0].trade_value == 200


def test_first_computable_day_starts_at_base_index_without_anchor() -> None:
    """앵커가 없으면 첫 계산 가능일의 직전 지수를 1000 으로 둔다. D1 은 t-1 이 없어 건너뛴다."""
    rows = [_row(1, D1, "100"), _row(1, D2, "105")]

    out = compute_theme_candles(1, rows, CAL, start=D1, end=D3, anchor_close=None)

    assert [c.trade_date for c in out] == [D2]
    assert out[0].close == BASE_INDEX * Decimal("1.05")


def test_anchor_close_is_chained() -> None:
    rows = [_row(1, D1, "100"), _row(1, D2, "105")]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D2, anchor_close=Decimal("2000"))

    assert out[0].close == Decimal("2100.0000")


def test_stock_missing_on_one_day_is_excluded_that_day() -> None:
    """2번 종목은 D2 가 없다. D2 는 1번만으로, D3 는 2번의 t-1(D2) 이 없으니 다시 1번만으로 계산."""
    rows = [
        _row(1, D1, "100"),
        _row(2, D1, "100"),
        _row(1, D2, "110"),
        _row(1, D3, "121"),
        _row(2, D3, "50"),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D3, anchor_close=None)

    assert out[0].close == Decimal("1100.0000")
    assert out[1].close == Decimal("1210.0000")
    assert out[1].volume == 10


def test_stock_without_listed_shares_is_excluded() -> None:
    rows = [
        _row(1, D1, "100"),
        _row(2, D1, "100", listed_shares=None),
        _row(1, D2, "110"),
        _row(2, D2, "50", listed_shares=None),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D2, anchor_close=None)

    assert out[0].close == Decimal("1100.0000")


def test_constituent_change_does_not_jump_index() -> None:
    """D3 에 3번 종목이 새로 들어와도 D3 지수는 D2 종가에 D3 하루 수익률만 곱한 값이다."""
    rows = [
        _row(1, D1, "100"),
        _row(1, D2, "110"),
        _row(1, D3, "110"),
        _row(3, D2, "500"),
        _row(3, D3, "500"),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D3, anchor_close=None)

    assert out[1].close == out[0].close


def test_day_with_no_participants_writes_no_row_and_chain_continues() -> None:
    d4 = date(2026, 9, 4)
    cal = [D1, D2, D3, d4]
    rows = [_row(1, D1, "100"), _row(1, D3, "100"), _row(1, d4, "110")]

    out = compute_theme_candles(1, rows, cal, start=D2, end=d4, anchor_close=Decimal("1500"))

    # D2: 1번의 D2 봉 없음 → 0 참여. D3: t-1 은 D2 인데 봉이 없어 역시 0 참여.
    # D4: D3 100 → D4 110 이라 앵커 1500 이 그대로 이어져 1650.
    assert len(out) == 1
    assert out[0].trade_date == d4
    assert out[0].close == Decimal("1650.0000")


def test_stock_with_return_beyond_price_limit_is_excluded_that_day() -> None:
    """2번의 D2 +400% 는 가격제한폭을 넘어 D2 에서만 빠지고, D3 에는 다시 참여한다."""
    rows = [
        _row(1, D1, "100", volume=10),
        _row(2, D1, "100", volume=7),
        _row(1, D2, "110", volume=10),
        _row(2, D2, "500", volume=7),
        _row(1, D3, "112.2", volume=10),
        _row(2, D3, "510", volume=7),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D3, anchor_close=None)

    # D2: 1번만 → 1000 × 1.10 = 1100, volume 도 1번만.
    assert out[0].close == Decimal("1100.0000")
    assert out[0].volume == 10
    # D3: 두 종목 모두 +2%, 동일 가중 → 1100 × 1.02 = 1122.
    assert out[1].close == Decimal("1122.0000")
    assert out[1].volume == 17


def test_return_exactly_at_price_limit_is_included() -> None:
    rows = [_row(1, D1, "100"), _row(1, D2, "130")]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D2, anchor_close=None)

    assert out[0].close == Decimal("1300.0000")


def test_ohlc_consistency_and_rounding() -> None:
    rows = [
        _row(1, D1, "100"),
        _row(1, D2, "103", open_="101", high="102", low="99"),
    ]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D2, anchor_close=None)

    c = out[0]
    assert c.open == Decimal("1010.0000")
    assert c.close == Decimal("1030.0000")
    # 원시 high(1020) 가 close(1030) 보다 낮으므로 high 는 close 로 보정된다.
    assert c.high == Decimal("1030.0000")
    assert c.low == Decimal("990.0000")
    assert c.low <= c.open <= c.high and c.low <= c.close <= c.high


def test_trade_value_none_when_all_missing() -> None:
    rows = [_row(1, D1, "100", trade_value=None), _row(1, D2, "100", trade_value=None)]

    out = compute_theme_candles(1, rows, CAL, start=D2, end=D2, anchor_close=None)

    assert out[0].trade_value is None
