from __future__ import annotations

import asyncio

from pipelines.themes.jobs.steps import (
    SOURCES,
    extract_source,
    load_themes,
    reset,
    validate_themes,
)

async def _run_async() -> None:
    """
    steps의 실행 단위를 CLI에서 순차로 조합한다.
    각 step이 자체적으로 자원을 열고 닫으므로 여기서는 순서만 보장한다.
    """
    await reset()

    nested = await asyncio.gather(
        *(extract_source(source_name) for source_name in SOURCES)
    )

    validated = await validate_themes(nested)
    await load_themes(validated)


def run() -> None:
    asyncio.run(_run_async())


if __name__ == "__main__":
    run()
