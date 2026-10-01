"""scripts/eval/issue_links 의 스크립트 모듈을 import 할 수 있게 경로를 잡는다.

스크립트는 `python scripts/eval/issue_links/xxx.py` 로 돌며 같은 폴더의 linkeval 을 형제
모듈로 import 한다. 테스트도 같은 방식으로 import 하도록 그 폴더를 sys.path 앞에 둔다.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts" / "eval" / "issue_links"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
