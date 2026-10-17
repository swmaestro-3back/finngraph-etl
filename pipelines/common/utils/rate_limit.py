from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass

# utils 는 설정(config) 의존이 없어야 해서 공통 get_logger 대신 표준 로거를 쓴다.
logger = logging.getLogger(__name__)


@dataclass
class FixedWindowRateLimiter:
    calls_per_second: int
    _window_started_at: float = 0
    _calls_in_window: int = 0

    def wait(self) -> None:
        if self.calls_per_second <= 0:
            return

        now = time.monotonic()
        if self._window_started_at == 0 or now - self._window_started_at >= 1:
            self._window_started_at = now
            self._calls_in_window = 0

        if self._calls_in_window >= self.calls_per_second:
            time.sleep(max(0, 1 - (now - self._window_started_at)))
            self._window_started_at = time.monotonic()
            self._calls_in_window = 0

        self._calls_in_window += 1


@dataclass
class BurstPacer:
    """지터 + 버스트-쿨다운 페이서. 안티어뷰징(IP 차단)이 있는 외부 사이트용이다.

    차단 탐지는 보통 두 신호를 본다 — 메트로놈처럼 일정한 요청 간격, 그리고 시간
    윈도우당 누적 호출량. 요청 사이를 랜덤 간격으로 벌려 전자를, burst_size 회마다
    길게 쉬어 후자를 무디게 한다.

    `FixedWindowRateLimiter`와 같은 wait() 프로토콜이라 서로 바꿔 끼울 수 있다.
    저쪽은 공식 API 의 명시된 한도(예: KIS 초당 N회)를 지키는 용도, 이쪽은 한도가
    공개되지 않은 사이트를 자극하지 않는 용도다.
    """

    min_interval_seconds: float
    max_interval_seconds: float
    burst_size: int
    min_cooldown_seconds: float
    max_cooldown_seconds: float
    _calls_in_burst: int = 0

    def wait(self) -> None:
        if self._calls_in_burst >= self.burst_size:
            cooldown = random.uniform(self.min_cooldown_seconds, self.max_cooldown_seconds)
            # 수십 초 침묵이 행(hang)처럼 보이지 않게 남긴다.
            logger.info("버스트 %d회 소진 — %.0f초 쿨다운", self.burst_size, cooldown)
            time.sleep(cooldown)
            self._calls_in_burst = 0

        time.sleep(random.uniform(self.min_interval_seconds, self.max_interval_seconds))
        self._calls_in_burst += 1
