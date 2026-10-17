"""job 집계 불변식 검사.

generate_events 의 "전체 = 결과의 합" 검사. job 본문과 떼어 따로 테스트한다.
"""

from __future__ import annotations

from collections.abc import Sequence


def check_total(stats: dict[str, int], total_key: str, part_keys: Sequence[str]) -> dict[str, int]:
    """stats[total_key] 가 part_keys 값의 합과 같은지 검사하고 stats 를 그대로 돌려준다."""

    accounted = sum(stats[key] for key in part_keys)
    if stats[total_key] != accounted:
        raise ValueError(f"집계 불일치: {total_key}={stats[total_key]} != 합={accounted} ({stats})")
    return stats
