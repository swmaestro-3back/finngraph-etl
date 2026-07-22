from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StockSymbol:
    symbol: str
    name: str
    market: str
    is_active: bool = True
