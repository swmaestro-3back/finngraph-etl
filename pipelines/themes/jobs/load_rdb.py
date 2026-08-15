from __future__ import annotations

import json
from pathlib import Path

from pipelines.common.logging import get_logger
from pipelines.themes.loaders.rdb import load_themes_to_rdb

logger = get_logger(__name__)


def run(validated_path: str) -> None:
    """validate가 저장한 validated.json을 읽어 RDB(themes/theme_stocks)에 적재한다."""

    themes = json.loads(Path(validated_path).read_text(encoding="utf-8"))
    result = load_themes_to_rdb(themes)

    print("\n" + "=" * 60)
    print("테마 RDB 적재 결과")
    print("=" * 60)
    print(f"- 적재 결과 {result}")
    print("작업 완료")
    print("=" * 60)


if __name__ == "__main__":
    import sys

    run(sys.argv[1])
