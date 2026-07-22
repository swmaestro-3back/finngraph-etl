from __future__ import annotations

import asyncio

from pipelines.common.http import http_client
from pipelines.themes.extractors import AntWinnerExtractor


async def main() -> None:
    http_client.start()
    try:
        extractor = AntWinnerExtractor()
        await extractor.run()
    finally:
        await http_client.stop()


if __name__ == "__main__":
    asyncio.run(main())
