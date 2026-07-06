from __future__ import annotations

from sqlalchemy.orm import Session

from pipelines.stocks.models import StockSymbol


def upsert_symbols(session: Session, symbols: list[StockSymbol]) -> int:
    raise NotImplementedError("Symbol upsert is not implemented yet.")

