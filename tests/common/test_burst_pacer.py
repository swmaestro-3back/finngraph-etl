"""BurstPacer 유닛 테스트.

실제로 잠들면 테스트가 느려지므로 sleep 을 기록만 하는 함수로 바꿔, 지터 슬립과
버스트 경계에서의 쿨다운 삽입이 맞는 횟수·범위로 일어나는지만 본다.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from pipelines.common.utils.rate_limit import BurstPacer


def _pacer(burst_size: int = 3) -> BurstPacer:
    return BurstPacer(
        min_interval_seconds=0.4,
        max_interval_seconds=1.2,
        burst_size=burst_size,
        min_cooldown_seconds=45,
        max_cooldown_seconds=90,
    )


class BurstPacerTest(unittest.TestCase):
    def test_sleeps_with_jitter_between_calls(self) -> None:
        pacer = _pacer()

        with patch("pipelines.common.utils.rate_limit.time.sleep") as sleep:
            pacer.wait()
            pacer.wait()

        self.assertEqual(sleep.call_count, 2)
        for call in sleep.call_args_list:
            self.assertTrue(0.4 <= call.args[0] <= 1.2)

    def test_cooldown_kicks_in_after_burst(self) -> None:
        pacer = _pacer(burst_size=3)

        with patch("pipelines.common.utils.rate_limit.time.sleep") as sleep:
            for _ in range(4):
                pacer.wait()

        # 3회 버스트 뒤 4번째 호출 직전에 쿨다운이 끼어든다: 지터 4회 + 쿨다운 1회.
        self.assertEqual(sleep.call_count, 5)
        cooldowns = [call.args[0] for call in sleep.call_args_list if call.args[0] >= 45]
        self.assertEqual(len(cooldowns), 1)
        self.assertTrue(45 <= cooldowns[0] <= 90)

    def test_burst_counter_resets_after_cooldown(self) -> None:
        pacer = _pacer(burst_size=2)

        with patch("pipelines.common.utils.rate_limit.time.sleep") as sleep:
            for _ in range(6):
                pacer.wait()

        # 2회마다 쿨다운: 3번째·5번째 호출 직전 두 번. 지터 6회 + 쿨다운 2회.
        cooldowns = [call.args[0] for call in sleep.call_args_list if call.args[0] >= 45]
        self.assertEqual(len(cooldowns), 2)
        self.assertEqual(sleep.call_count, 8)


if __name__ == "__main__":
    unittest.main()
