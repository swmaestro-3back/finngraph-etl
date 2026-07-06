from __future__ import annotations

from pipelines.common.types import MinuteCandle


def aggregate_1m_to_5m(candles: list[MinuteCandle]) -> list[MinuteCandle]:
    raise NotImplementedError("1m to 5m candle aggregation is not implemented yet.")
