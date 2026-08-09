from __future__ import annotations

from datetime import datetime

# Airflow는 보통 실제 서버에만 설치되어 있고, 개발 환경에는 설치 안되어 있는 경우가 많다.
# 따라서 아래와 같이 Airflow가 설치되어 있지 않는 개발환경이라면
# import 시 터지면 안되므로 Fallback을 시켜준다
try:
    from airflow.sdk import dag, task
except ImportError:
    # Allows syntax checks without Airflow installed.
    dag = None
    task = None


if dag and task:

    @dag(
        dag_id="health_check",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        tags=["health"],
    )
    def health_check():
        @task
        def ping() -> str:
            return "ok"

        ping()

    health_check()
