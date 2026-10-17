from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from pipelines.market_calendar.models import CalendarEvent, IpoOffering, MarketDay
from pipelines.market_calendar.transformers.parsing import (
    clean_name,
    clean_ticker,
    dedupe_rows,
    parse_amount,
    parse_date,
    parse_range,
)

SOURCE_DIVIDEND = "KSD_DIVIDEND"
SOURCE_BONUS = "KSD_BONUS"
SOURCE_RIGHTS = "KSD_RIGHTS"
SOURCE_AGM = "KSD_AGM"

ONE_DAY = timedelta(days=1)


def _last_open_on_or_before(target: date, days: dict[date, MarketDay]) -> tuple[date, bool]:
    cursor = target
    while cursor in days:
        if days[cursor].is_open:
            return cursor, False
        cursor -= ONE_DAY
    while cursor.weekday() >= 5:
        cursor -= ONE_DAY
    return cursor, True


def previous_open_day(target: date, days: dict[date, MarketDay]) -> tuple[date, bool]:
    effective, effective_estimated = _last_open_on_or_before(target, days)
    ex_date, ex_estimated = _last_open_on_or_before(effective - ONE_DAY, days)
    return ex_date, effective_estimated or ex_estimated


def _label(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _identity(row: dict[str, Any], basis_field: str) -> tuple[str, str, date] | None:
    ticker = clean_ticker(row.get("sht_cd"))
    basis = parse_date(row.get(basis_field))
    if not ticker or basis is None:
        return None
    return ticker, clean_name(row.get("isin_name")) or ticker, basis


def to_dividend_events(
    rows: list[dict[str, Any]], days: dict[date, MarketDay]
) -> list[CalendarEvent]:
    events: list[CalendarEvent] = []
    for row in dedupe_rows(rows):
        identity = _identity(row, "record_date")
        if identity is None:
            continue
        ticker, name, record_date = identity
        label = _label(row.get("divi_kind"))
        common = {
            "ticker": ticker,
            "stock_name": name,
            "source": SOURCE_DIVIDEND,
            "source_key": f"{ticker}|{record_date:%Y%m%d}|{label or ''}",
            "basis_date": record_date,
            "amount": parse_amount(row.get("per_sto_divi_amt")),
            "ratio": parse_amount(row.get("divi_rate")),
            "label": label,
        }
        ex_date, estimated = previous_open_day(record_date, days)
        events.append(
            CalendarEvent(event_date=record_date, kind="DIV_RECORD", detail={"raw": row}, **common)
        )
        events.append(
            CalendarEvent(
                event_date=ex_date,
                kind="DIV_EX",
                detail={"raw": row, "estimated": estimated},
                **common,
            )
        )
        pay_date = parse_date(row.get("divi_pay_dt"))
        if pay_date is not None:
            events.append(
                CalendarEvent(event_date=pay_date, kind="DIV_PAY", detail={"raw": row}, **common)
            )
    return events


def to_bonus_events(rows: list[dict[str, Any]]) -> list[CalendarEvent]:
    events: list[CalendarEvent] = []
    for row in dedupe_rows(rows):
        identity = _identity(row, "record_date")
        if identity is None:
            continue
        ticker, name, record_date = identity
        common = {
            "ticker": ticker,
            "stock_name": name,
            "source": SOURCE_BONUS,
            "source_key": f"{ticker}|{record_date:%Y%m%d}",
            "basis_date": record_date,
            "ratio": parse_amount(row.get("fix_rate")),
            "detail": {"raw": row},
        }
        ex_date = parse_date(row.get("right_dt"))
        if ex_date is not None:
            events.append(CalendarEvent(event_date=ex_date, kind="BONUS_EX", **common))
        listing_date = parse_date(row.get("list_date"))
        if listing_date is not None:
            events.append(CalendarEvent(event_date=listing_date, kind="BONUS_LIST", **common))
    return events


def to_rights_events(
    rows: list[dict[str, Any]], basis_field: str = "record_date"
) -> list[CalendarEvent]:
    events: list[CalendarEvent] = []
    for row in dedupe_rows(rows):
        identity = _identity(row, basis_field)
        if identity is None:
            continue
        ticker, name, basis_date = identity
        record_date = parse_date(row.get("record_date")) or basis_date
        common = {
            "ticker": ticker,
            "stock_name": name,
            "source": SOURCE_RIGHTS,
            "source_key": f"{ticker}|{record_date:%Y%m%d}",
            "basis_date": basis_date,
            "amount": parse_amount(row.get("fix_price")),
            "ratio": parse_amount(row.get("fix_rate")),
            "detail": {"raw": row},
        }
        ex_date = parse_date(row.get("right_dt"))
        if ex_date is not None:
            events.append(CalendarEvent(event_date=ex_date, kind="RIGHTS_EX", **common))
        subscribe_start, subscribe_end = parse_range(row.get("sub_term"))
        if subscribe_start is not None:
            events.append(
                CalendarEvent(
                    event_date=subscribe_start,
                    kind="RIGHTS_SUBSCRIBE",
                    end_date=subscribe_end,
                    **common,
                )
            )
        listing_date = parse_date(row.get("list_date"))
        if listing_date is not None:
            events.append(CalendarEvent(event_date=listing_date, kind="RIGHTS_LIST", **common))
    return events


def to_agm_events(rows: list[dict[str, Any]]) -> list[CalendarEvent]:
    groups: dict[tuple[str, date, date, str], list[dict[str, Any]]] = {}
    names: dict[tuple[str, date, date, str], str] = {}
    for row in dedupe_rows(rows):
        identity = _identity(row, "record_date")
        meeting_date = parse_date(row.get("gen_meet_dt"))
        if identity is None or meeting_date is None:
            continue
        ticker, name, record_date = identity
        key = (ticker, record_date, meeting_date, str(row.get("gen_meet_type") or "").strip())
        groups.setdefault(key, []).append(row)
        names.setdefault(key, name)

    events: list[CalendarEvent] = []
    for key, grouped in groups.items():
        ticker, record_date, meeting_date, meeting_type = key
        agenda: list[str] = []
        for row in grouped:
            item = str(row.get("agenda") or "").strip()
            if item and item not in agenda:
                agenda.append(item)
        events.append(
            CalendarEvent(
                event_date=meeting_date,
                kind="AGM",
                ticker=ticker,
                stock_name=names[key],
                source=SOURCE_AGM,
                source_key=f"{ticker}|{record_date:%Y%m%d}|{meeting_date:%Y%m%d}|{meeting_type}",
                basis_date=record_date,
                label=meeting_type or None,
                detail={
                    "agenda": agenda,
                    "agenda_truncated": any(row.get("_truncated") for row in grouped),
                    "raw": grouped,
                },
            )
        )
    return events


def to_ipo_offerings(rows: list[dict[str, Any]]) -> list[IpoOffering]:
    offerings: list[IpoOffering] = []
    for row in dedupe_rows(rows):
        identity = _identity(row, "record_date")
        subscribe_start, subscribe_end = parse_range(row.get("subscr_dt"))
        if identity is None or subscribe_start is None:
            continue
        ticker, name, basis_date = identity
        offerings.append(
            IpoOffering(
                ticker=ticker,
                name=name,
                subscr_start=subscribe_start,
                subscr_end=subscribe_end or subscribe_start,
                offer_price=parse_amount(row.get("fix_subscr_pri")),
                pay_date=parse_date(row.get("pay_dt")),
                refund_date=parse_date(row.get("refund_dt")),
                listing_date=parse_date(row.get("list_dt")),
                lead_managers=_label(row.get("lead_mgr")),
                basis_date=basis_date,
                detail={"raw": row},
            )
        )
    return offerings
