"""job 집계 불변식 검사.

sync_events 와 generate_events 가 각자 다른 키로 같은 검사("전체 = 결과의 합")를 하므로
공용으로 둔다.
"""

from __future__ import annotations

from collections.abc import Sequence


def check_total(stats: dict[str, int], total_key: str, part_keys: Sequence[str]) -> dict[str, int]:
    """stats[total_key] 가 part_keys 값의 합과 같은지 검사하고 stats 를 그대로 돌려준다."""

    accounted = sum(stats[key] for key in part_keys)
    if stats[total_key] != accounted:
        raise ValueError(f"집계 불일치: {total_key}={stats[total_key]} != 합={accounted} ({stats})")
    return stats
