from __future__ import annotations

import asyncio

from pipelines.common.clients.http import http_client
from pipelines.themes.extractors import JudalExtractor


async def main() -> None:
    async with http_client:
        extractor = JudalExtractor()
        await extractor.run()


if __name__ == "__main__":
    asyncio.run(main())
