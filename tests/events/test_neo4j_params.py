"""_bolt_datetime / _bolt_params 단위 테스트.

neo4j 드라이버가 ZoneInfo tzinfo 를 가진 datetime 을 패킹할 때 CPython 3.14 에서
세그폴트가 나므로 고정 오프셋으로 바꾸는 정규화 헬퍼를 검증한다. Neo4j 를 건드리지
않는다 — integration 마커 없음.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipelines.events.loaders.neo4j import _bolt_datetime, _bolt_params

SEOUL = ZoneInfo("Asia/Seoul")


def test_zoneinfo_datetime_becomes_fixed_offset_same_instant() -> None:
    value = datetime(2026, 9, 1, 9, 0, tzinfo=SEOUL)

    result = _bolt_datetime(value)

    assert isinstance(result.tzinfo, timezone)
    assert not isinstance(result.tzinfo, type(SEOUL))
    assert result == value
    assert (result.year, result.month, result.day) == (2026, 9, 1)
    assert (result.hour, result.minute) == (9, 0)
    assert result.utcoffset() == timedelta(hours=9)


def test_fixed_offset_datetime_is_unchanged() -> None:
    value = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)

    assert _bolt_datetime(value) is value


def test_naive_datetime_is_unchanged() -> None:
    value = datetime(2026, 9, 1, 0, 0)

    assert _bolt_datetime(value) is value


def test_bolt_params_only_converts_datetime_values() -> None:
    seoul_value = datetime(2026, 9, 1, 9, 0, tzinfo=SEOUL)
    payload = {
        "when": seoul_value,
        "count": 3,
        "names": ["a", "b"],
        "note": None,
    }

    result = _bolt_params(payload)

    assert result["when"] == seoul_value
    assert isinstance(result["when"].tzinfo, timezone)
    assert result["count"] == 3
    assert result["count"] is payload["count"]
    assert result["names"] == ["a", "b"]
    assert result["names"] is payload["names"]
    assert result["note"] is None
