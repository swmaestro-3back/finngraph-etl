from __future__ import annotations

from tenacity import retry, stop_after_attempt, wait_exponential


def retry_external_call():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )

