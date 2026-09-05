"""check_total — 집계 불변식."""

from __future__ import annotations

import pytest

from pipelines.events.stats import check_total


def test_passes_and_returns_same_dict_when_parts_sum_to_total():
    stats = {"scanned": 5, "a": 2, "b": 3, "extra": 99}

    assert check_total(stats, "scanned", ("a", "b")) is stats


def test_raises_on_mismatch():
    with pytest.raises(ValueError, match="집계 불일치"):
        check_total({"scanned": 5, "a": 2, "b": 2}, "scanned", ("a", "b"))
