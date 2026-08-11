"""배치 처리 공통 유틸.

종목별 조회 API를 도는 job은 전부 같은 모양이다 — 대상 목록을 청크로 잘라, 청크마다
수집하고 커밋한다. 한 트랜잭션에 수천 종목을 몰면 실패 시 전부 되돌아가고, 종목마다
커밋하면 왕복이 너무 잦다.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")


def chunked(items: Iterable[T], size: int) -> Iterator[list[T]]:
    """items를 size 크기 리스트로 잘라 순서대로 내보낸다.

    Args:
        items (Iterable[T]): 원본.
        size (int): 청크 크기. 1 이상이어야 한다.

    Yields:
        list[T]: 최대 size개짜리 청크. 마지막 청크는 더 작을 수 있다.
    """

    if size < 1:
        raise ValueError(f"size는 1 이상이어야 한다: {size}")

    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
