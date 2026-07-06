from __future__ import annotations

import argparse
import importlib
from collections.abc import Callable


def load_job(path: str) -> Callable[[], None]:
    module_name, function_name = path.rsplit(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an ETL job by import path.")
    parser.add_argument("job", help="Example: pipelines.stocks.jobs.collect_intraday_1m:run")
    args = parser.parse_args()

    job = load_job(args.job)
    job()


if __name__ == "__main__":
    main()

