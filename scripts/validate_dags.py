#!/usr/bin/env python
"""Airflow DAG 파싱 검증 스크립트.

`dags/` 아래 모든 DAG 파일을 Airflow DagBag으로 실제 로드해서 import 에러가 없는지 확인한다.
DAG 파일은 `try/except ImportError`로 airflow 미설치 환경에서도 compile은 되지만, 실제 DAG 구조가
올바른지(순환 참조, 잘못된 인자, 깨진 import 등)는 airflow가 설치된 상태에서 DagBag으로 파싱해야
검증된다. CI의 dag-validation job이 이 스크립트를 호출한다.

로컬 실행:
    pip install -e ".[airflow]"
    python scripts/validate_dags.py

성공 시 발견한 dag_id 목록을 출력하고 0으로 종료, import 에러가 하나라도 있으면 1로 종료한다.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"


def main() -> int:
    if not DAGS_DIR.is_dir():
        print(f"dags 디렉터리를 찾을 수 없음: {DAGS_DIR}", file=sys.stderr)
        return 1

    # airflow import 전에 환경을 격리한다. AIRFLOW_HOME을 임시 디렉터리로 두어 로컬/CI 상태를
    # 오염시키지 않고, 예제 DAG 로딩과 metadata DB 접근이 필요 없는 파싱 전용 모드로 실행한다.
    tmp_home = tempfile.mkdtemp(prefix="airflow-dagcheck-")
    os.environ.setdefault("AIRFLOW_HOME", tmp_home)
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

    try:
        from airflow.models.dagbag import DagBag
    except ImportError as exc:  # pragma: no cover - 안내용 경로
        print(
            "apache-airflow가 설치되어 있지 않습니다. "
            '`pip install -e ".[airflow]"` 후 실행하세요.\n'
            f"원인: {exc}",
            file=sys.stderr,
        )
        return 1

    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    if dag_bag.import_errors:
        print(f"DAG import 에러 {len(dag_bag.import_errors)}건 발견:", file=sys.stderr)
        for filename, traceback in dag_bag.import_errors.items():
            print(f"\n--- {filename} ---", file=sys.stderr)
            print(traceback, file=sys.stderr)
        return 1

    dag_ids = sorted(dag_bag.dag_ids)
    if not dag_ids:
        print("경고: 로드된 DAG가 없습니다. dags/ 내용을 확인하세요.", file=sys.stderr)
        # DAG가 0개인 것은 import 에러는 아니므로 실패로 보지 않는다(초기 단계 허용).

    print(f"DAG import 에러 없음. 로드된 DAG {len(dag_ids)}개: {dag_ids}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
