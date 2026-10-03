from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from pipelines.common.clients.kis import KisClient, KisPage, get_kis_client
from pipelines.market_calendar.transformers.parsing import parse_date

KSD_PREFIX = "/uapi/domestic-stock/v1/ksdinfo"
TRUNCATED = ("F", "M")
BASIS_FIELD = "record_date"

SplitKey = tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class KsdEndpoint:
    name: str
    path: str
    tr_id: str
    extra_params: dict[str, str] = field(default_factory=dict)
    overflow_splits: tuple[dict[str, str], ...] = ()


DIVIDEND = KsdEndpoint(
    "dividend",
    f"{KSD_PREFIX}/dividend",
    "HHKDB669102C0",
    {"GB1": "0", "HIGH_GB": ""},
    ({"GB1": "1"}, {"GB1": "2"}),
)
BONUS_ISSUE = KsdEndpoint("bonus_issue", f"{KSD_PREFIX}/bonus-issue", "HHKDB669101C0")
RIGHTS_ISSUE = KsdEndpoint(
    "rights_issue", f"{KSD_PREFIX}/paidin-capin", "HHKDB669100C0", {"GB1": "2"}
)
SHAREHOLDER_MEETING = KsdEndpoint(
    "shareholder_meeting", f"{KSD_PREFIX}/sharehld-meet", "HHKDB669111C0"
)
PUBLIC_OFFERING = KsdEndpoint("public_offering", f"{KSD_PREFIX}/pub-offer", "HHKDB669108C0")


@dataclass
class FetchStats:
    calls: int = 0
    overflow_dates: list[date] = field(default_factory=list)
    failed_dates: list[date] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    truncated: list[str] = field(default_factory=list)


def fetch_ksd_rows(
    endpoint: KsdEndpoint,
    start: date,
    end: date,
    tickers_provider: Callable[[], list[str]],
    client: KisClient | None = None,
    stats: FetchStats | None = None,
) -> list[dict[str, Any]]:
    client = client or get_kis_client()
    stats = stats if stats is not None else FetchStats()
    cached: list[list[str]] = []

    def tickers() -> list[str]:
        if not cached:
            cached.append(tickers_provider())
        return cached[0]

    crowded: dict[SplitKey, set[date]] = {}
    rows = _fetch(endpoint, start, end, client, stats, crowded)
    for split_key, days in crowded.items():
        rows.extend(_sweep(endpoint, start, end, split_key, days, tickers, client, stats))
    return rows


def _params(
    endpoint: KsdEndpoint,
    start: date,
    end: date,
    ticker: str = "",
    split: dict[str, str] | None = None,
) -> dict[str, str]:
    return {
        "CTS": "",
        "F_DT": start.strftime("%Y%m%d"),
        "T_DT": end.strftime("%Y%m%d"),
        "SHT_CD": ticker,
        **endpoint.extra_params,
        **(split or {}),
    }


def _rows(page: KisPage) -> list[dict[str, Any]]:
    rows = page.data.get("output1") or []
    return [rows] if isinstance(rows, dict) else list(rows)


def _request(
    endpoint: KsdEndpoint, params: dict[str, str], client: KisClient, stats: FetchStats
) -> KisPage:
    stats.calls += 1
    return client.request_page(endpoint.path, endpoint.tr_id, params)


def _fetch(
    endpoint: KsdEndpoint,
    start: date,
    end: date,
    client: KisClient,
    stats: FetchStats,
    crowded: dict[SplitKey, set[date]],
) -> list[dict[str, Any]]:
    page = _request(endpoint, _params(endpoint, start, end), client, stats)
    if page.tr_cont not in TRUNCATED:
        return _rows(page)
    if start < end:
        middle = start + (end - start) // 2
        return _fetch(endpoint, start, middle, client, stats, crowded) + _fetch(
            endpoint, middle + timedelta(days=1), end, client, stats, crowded
        )
    stats.overflow_dates.append(start)
    if not endpoint.overflow_splits:
        crowded.setdefault((), set()).add(start)
        return []
    rows: list[dict[str, Any]] = []
    for split in endpoint.overflow_splits:
        split_page = _request(endpoint, _params(endpoint, start, start, split=split), client, stats)
        if split_page.tr_cont in TRUNCATED:
            crowded.setdefault(tuple(sorted(split.items())), set()).add(start)
        else:
            rows.extend(_rows(split_page))
    return rows


def _sweep(
    endpoint: KsdEndpoint,
    start: date,
    end: date,
    split_key: SplitKey,
    days: set[date],
    tickers: Callable[[], list[str]],
    client: KisClient,
    stats: FetchStats,
) -> list[dict[str, Any]]:
    split = dict(split_key) or None
    rows: list[dict[str, Any]] = []
    try:
        for ticker in tickers():
            rows.extend(
                _fetch_ticker(endpoint, min(days), max(days), ticker, split, days, client, stats)
            )
    except Exception as exc:
        failed = sorted(days)
        stats.failed_dates.extend(failed)
        stats.errors.append(f"{endpoint.name} {split or {}} {failed}: {type(exc).__name__}: {exc}")
        return []
    return rows


def _fetch_ticker(
    endpoint: KsdEndpoint,
    start: date,
    end: date,
    ticker: str,
    split: dict[str, str] | None,
    days: set[date],
    client: KisClient,
    stats: FetchStats,
) -> list[dict[str, Any]]:
    if not any(start <= day <= end for day in days):
        return []
    page = _request(endpoint, _params(endpoint, start, end, ticker, split), client, stats)
    rows = [row for row in _rows(page) if parse_date(row.get(BASIS_FIELD)) in days]
    if page.tr_cont not in TRUNCATED:
        return rows
    if start < end:
        middle = start + (end - start) // 2
        return _fetch_ticker(
            endpoint, start, middle, ticker, split, days, client, stats
        ) + _fetch_ticker(
            endpoint, middle + timedelta(days=1), end, ticker, split, days, client, stats
        )
    stats.truncated.append(f"{ticker} {start}")
    return [{**row, "_truncated": True} for row in rows]
